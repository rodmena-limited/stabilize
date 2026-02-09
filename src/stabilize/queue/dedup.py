"""
Bloom filter for probabilistic message deduplication.

Provides fast, memory-efficient deduplication for message processing
with zero false negatives and configurable false positive rate.

Usage:
    from stabilize.queue.dedup import BloomDeduplicator

    dedup = BloomDeduplicator(expected_items=100_000, false_positive_rate=0.001)

    # Check if message might have been seen
    if dedup.maybe_seen(message.message_id):
        # Might be duplicate - check database to confirm
        if store.is_message_processed(message.message_id):
            return  # Skip duplicate
    else:
        # Definitely not seen - process without DB check
        pass

    # Mark as seen after processing
    dedup.mark_seen(message.message_id)
"""

from __future__ import annotations

import hashlib
import math
import threading
import time


class BloomDeduplicator:
    """Probabilistic message dedup using bloom filter.

    Zero false negatives: if filter says "not seen", it's definitely new.
    Small false positive rate: if filter says "seen", might need DB check.

    The bloom filter uses multiple hash functions (via hashlib) to set
    bits in a bit array. This provides O(1) lookup and insertion with
    configurable accuracy.

    Thread Safety:
        All methods are thread-safe. Uses a lock for consistency.

    Attributes:
        expected_items: Expected number of unique items
        false_positive_rate: Target false positive rate (0.0 to 1.0)

    Example:
        dedup = BloomDeduplicator(
            expected_items=100_000,
            false_positive_rate=0.001,  # 0.1% false positives
        )

        # Fast path: definitely not seen
        if not dedup.maybe_seen("msg-123"):
            process(message)  # Skip DB check
            dedup.mark_seen("msg-123")

        # Slow path: might be duplicate
        else:
            if not db.is_processed("msg-123"):
                process(message)
                dedup.mark_seen("msg-123")
    """

    def __init__(
        self,
        expected_items: int = 100_000,
        false_positive_rate: float = 0.001,
    ) -> None:
        """Initialize with expected capacity and target FP rate.

        Args:
            expected_items: Expected number of unique message IDs
            false_positive_rate: Target false positive rate (default: 0.1%)

        Raises:
            ValueError: If expected_items <= 0 or false_positive_rate not in (0, 1)
        """
        if expected_items <= 0:
            raise ValueError("expected_items must be positive")
        if not (0 < false_positive_rate < 1):
            raise ValueError("false_positive_rate must be between 0 and 1")

        self._expected_items = expected_items
        self._fp_rate = false_positive_rate

        # Calculate optimal bit array size and number of hash functions
        # m = -n * ln(p) / (ln(2)^2)
        # k = (m/n) * ln(2)
        self._size = self._optimal_size(expected_items, false_positive_rate)
        self._num_hashes = self._optimal_hashes(self._size, expected_items)

        # Initialize bit array as bytearray
        # Each byte holds 8 bits, so we need ceil(size / 8) bytes
        self._bit_array = bytearray((self._size + 7) // 8)
        self._items_added = 0
        self._creation_time = time.monotonic()
        self._max_age_seconds = 86400.0  # 24 hours default
        self._lock = threading.Lock()

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Calculate optimal bit array size.

        Args:
            n: Expected number of items
            p: Target false positive rate

        Returns:
            Optimal number of bits
        """
        # m = -n * ln(p) / (ln(2)^2)
        m = -n * math.log(p) / (math.log(2) ** 2)
        return int(math.ceil(m))

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        """Calculate optimal number of hash functions.

        Args:
            m: Bit array size
            n: Expected number of items

        Returns:
            Optimal number of hash functions
        """
        # k = (m/n) * ln(2)
        k = (m / n) * math.log(2)
        return max(1, int(math.ceil(k)))

    def _get_hash_positions(self, item: str) -> list[int]:
        """Get bit positions for an item using double hashing.

        Uses MD5 and SHA1 to generate initial hashes, then combines
        them for additional hash functions (double hashing technique).

        Args:
            item: The item to hash

        Returns:
            List of bit positions to check/set
        """
        item_bytes = item.encode("utf-8")

        # Get two independent hashes
        h1 = int(hashlib.md5(item_bytes).hexdigest(), 16)
        h2 = int(hashlib.sha1(item_bytes).hexdigest(), 16)

        # Generate k hash positions using double hashing
        positions = []
        for i in range(self._num_hashes):
            # h(i) = (h1 + i * h2) mod m
            pos = (h1 + i * h2) % self._size
            positions.append(pos)

        return positions

    def _get_bit(self, pos: int) -> bool:
        """Get the value of a bit at position."""
        byte_idx = pos // 8
        bit_idx = pos % 8
        return bool(self._bit_array[byte_idx] & (1 << bit_idx))

    def _set_bit(self, pos: int) -> None:
        """Set the bit at position to 1."""
        byte_idx = pos // 8
        bit_idx = pos % 8
        self._bit_array[byte_idx] |= 1 << bit_idx

    def maybe_seen(self, message_id: str) -> bool:
        """Return True if message might have been seen (check DB to confirm).

        False means definitely not seen (no DB check needed).
        True means possibly seen (requires DB check to confirm).

        Args:
            message_id: The unique message ID to check

        Returns:
            True if message might have been seen, False if definitely new.
        """
        positions = self._get_hash_positions(message_id)

        with self._lock:
            for pos in positions:
                if not self._get_bit(pos):
                    return False
            return True

    def mark_seen(self, message_id: str) -> None:
        """Mark message as seen in the filter.

        Args:
            message_id: The unique message ID to mark
        """
        positions = self._get_hash_positions(message_id)

        with self._lock:
            for pos in positions:
                self._set_bit(pos)
            self._items_added += 1

    def reset(self) -> None:
        """Clear the filter (e.g., on rotation schedule).

        Use when the filter is too full or on a time-based schedule.
        """
        with self._lock:
            self._bit_array = bytearray((self._size + 7) // 8)
            self._items_added = 0
            self._creation_time = time.monotonic()

    @property
    def fill_ratio(self) -> float:
        """How full the filter is (0.0 to 1.0).

        Reset when > 0.7 to maintain accuracy.

        Returns:
            Ratio of set bits to total bits.
        """
        with self._lock:
            set_bits = sum(bin(b).count("1") for b in self._bit_array)
            return set_bits / self._size

    @property
    def items_added(self) -> int:
        """Number of items added to the filter."""
        with self._lock:
            return self._items_added

    @property
    def age_seconds(self) -> float:
        """Age of the filter in seconds since creation or last reset."""
        return time.monotonic() - self._creation_time

    @property
    def expected_items(self) -> int:
        """Expected capacity of the filter."""
        return self._expected_items

    @property
    def size_bytes(self) -> int:
        """Size of the filter in bytes."""
        return len(self._bit_array)

    @property
    def estimated_false_positive_rate(self) -> float:
        """Estimated current false positive rate.

        This increases as the filter fills up.

        Returns:
            Estimated false positive rate based on current fill.
        """
        with self._lock:
            if self._items_added == 0:
                return 0.0

            # p' = (1 - e^(-kn/m))^k
            exponent = -self._num_hashes * self._items_added / self._size
            return (1 - math.exp(exponent)) ** self._num_hashes

    def should_reset(self, threshold: float = 0.7) -> bool:
        """Check if filter should be reset based on fill ratio or age.

        Args:
            threshold: Fill ratio threshold (default: 0.7)

        Returns:
            True if fill ratio exceeds threshold or filter is older than max_age.
        """
        age = time.monotonic() - self._creation_time
        if age > self._max_age_seconds:
            return True
        return self.fill_ratio > threshold


# Global deduplicator instance
_deduplicator: BloomDeduplicator | None = None


def get_deduplicator(
    expected_items: int = 100_000,
    false_positive_rate: float = 0.001,
) -> BloomDeduplicator:
    """Get the global deduplicator instance.

    Creates a new deduplicator if one doesn't exist.

    Args:
        expected_items: Expected number of unique items (used only on creation)
        false_positive_rate: Target FP rate (used only on creation)

    Returns:
        The global BloomDeduplicator instance.
    """
    global _deduplicator
    if _deduplicator is None:
        _deduplicator = BloomDeduplicator(
            expected_items=expected_items,
            false_positive_rate=false_positive_rate,
        )
    return _deduplicator


def reset_deduplicator() -> None:
    """Reset the global deduplicator instance.

    Useful for testing.
    """
    global _deduplicator
    _deduplicator = None

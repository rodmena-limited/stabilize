"""Audit findings: SSRF address-classification bypasses and credential
disclosure through logs and persisted workflow state."""

from __future__ import annotations

import pytest

from stabilize.redaction import redact_db_url, redact_userinfo
from stabilize.tasks.http.task import _validate_url_safety

SECRET = "AUDITSECRET777"


class TestSsrfMustBlock:
    @pytest.mark.parametrize(
        "url",
        [
            "http://[::ffff:127.0.0.1]/x",
            "http://[::ffff:169.254.169.254]/x",
            "http://[::ffff:10.0.0.1]/x",
            "http://[::ffff:192.168.1.1]/x",
            "http://0.0.0.0/x",
            "http://[::]/x",
            "http://100.64.0.1/x",
            "http://224.0.0.1/x",
            "http://240.0.0.1/x",
            "http://[fe80::1]/x",
            "http://[fc00::1]/x",
            "http://127.0.0.1/x",
            "http://169.254.169.254/latest/meta-data/",
        ],
    )
    def test_blocked_addresses(self, url: str) -> None:
        with pytest.raises(ValueError):
            _validate_url_safety(url)

    @pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/x", "gopher://x/y"])
    def test_only_http_schemes_are_permitted(self, url: str) -> None:
        with pytest.raises(ValueError):
            _validate_url_safety(url)

    def test_unresolvable_host_fails_closed(self) -> None:
        with pytest.raises(ValueError):
            _validate_url_safety("http://no-such-host.invalid/x")


class TestSsrfMustAllow:
    """Without these the block tests above pass vacuously."""

    @pytest.mark.parametrize(
        "url", ["https://example.com/x", "http://example.com/x", "http://93.184.216.34/x"]
    )
    def test_public_destinations_are_permitted(self, url: str) -> None:
        _validate_url_safety(url)


class TestRedactUserinfo:
    def test_known_positive_passwordless_url_is_untouched(self) -> None:
        assert redact_userinfo("https://host/path") == "https://host/path"

    @pytest.mark.parametrize(
        "url",
        [
            f"https://user:{SECRET}@host/path",
            f"http://user:{SECRET}@host:8080/path?q=1",
            f"https://user:{SECRET}/slash@host/path",
            f"https://user:{SECRET}@a@host/path",
        ],
    )
    def test_secret_never_survives(self, url: str) -> None:
        assert SECRET not in redact_userinfo(url)

    def test_port_is_not_mistaken_for_a_password(self) -> None:
        assert redact_userinfo("https://host:8443/path") == "https://host:8443/path"


class TestRedactDbUrlHostlessForms:
    @pytest.mark.parametrize(
        "url",
        [
            f"postgresql://u:{SECRET}",
            f"postgresql://u:{SECRET}/db",
            f"password:{SECRET}",
        ],
    )
    def test_dsn_missing_its_host_still_redacts(self, url: str) -> None:
        assert SECRET not in redact_db_url(url)

    @pytest.mark.parametrize(
        "url", ["postgresql://host:5432/db", "postgresql://host:5432", "sqlite:///tmp/a.db"]
    )
    def test_ports_and_paths_are_preserved(self, url: str) -> None:
        assert redact_db_url(url) == url

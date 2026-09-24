"""A message that fails its field contract does not carry the rejected value (#58)."""

from __future__ import annotations

import traceback

import pytest

from stabilize.queue.messages import MessageContractError, create_message_from_dict

SECRET = "tok=Zq7SentinelPw9xK"


def _mistyped_signal() -> dict[str, object]:
    return {
        "execution_type": "workflow",
        "execution_id": "e1",
        "stage_id": "s1",
        "signal_name": "approve",
        "signal_data": SECRET,
    }


def test_detector_can_see_the_secret() -> None:
    assert SECRET in str(_mistyped_signal())


def test_contract_error_names_the_field_without_its_value() -> None:
    with pytest.raises(MessageContractError) as raised:
        create_message_from_dict("SignalStage", _mistyped_signal())
    assert "signal_data" in str(raised.value)
    assert SECRET not in str(raised.value)


def test_the_logged_traceback_does_not_carry_the_rejected_value() -> None:
    with pytest.raises(MessageContractError) as raised:
        create_message_from_dict("SignalStage", _mistyped_signal())
    assert SECRET not in "".join(traceback.format_exception(raised.value))

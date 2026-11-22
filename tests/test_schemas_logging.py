import io
import json
import logging

import pytest
from pydantic import ValidationError

from recorder_helpers import depth_frame, trade_frame
from sentinalops.data.recorder.messages import parse_message
from sentinalops.logging import JsonFormatter
from sentinalops.schemas import (
    DepthSnapshot,
    FeatureTick,
    OutcomeLabel,
    ScoreRequest,
    StressPrediction,
    TradeEvent,
)


def test_trade_event_round_trips_with_the_recorder_row():
    kind, row = parse_message(trade_frame(7), recv_time_ms=1_700_000_000_000)
    ev = TradeEvent.from_row(row)
    assert ev.to_row() == row
    assert TradeEvent.model_validate_json(ev.model_dump_json()) == ev
    assert ev.is_buy is (not row["buyer_is_maker"])


@pytest.mark.parametrize(
    "patch", [{"price": 0.0}, {"quantity": -1.0}, {"last_trade_id": 0, "first_trade_id": 5},
              {"symbol": "btcusdt"}, {"price": float("inf")}, {"extra": 1}]
)
def test_trade_event_rejects_bad_values(patch):
    _, row = parse_message(trade_frame(7), recv_time_ms=1_700_000_000_000)
    with pytest.raises(ValidationError):
        TradeEvent(**{**row, **patch})


def test_depth_snapshot_round_trips_with_the_recorder_row():
    _, row = parse_message(depth_frame(42), recv_time_ms=1_700_000_000_000)
    snap = DepthSnapshot.from_row(row)
    assert snap.to_row() == row
    assert DepthSnapshot.model_validate_json(snap.model_dump_json()) == snap
    assert snap.mid == pytest.approx((row["bid_px_1"] + row["ask_px_1"]) / 2)


@pytest.mark.parametrize(
    "patch",
    [
        {"bid_px_1": 100.02},  # crossed: best bid above best ask
        {"bid_px_2": 100.00},  # bids not strictly decreasing
        {"ask_px_3": 100.01},  # asks not strictly increasing
        {"ask_qty_1": -1.0},
    ],
)
def test_depth_snapshot_rejects_a_broken_book(patch):
    _, row = parse_message(depth_frame(42), recv_time_ms=1_700_000_000_000)
    with pytest.raises(ValidationError):
        DepthSnapshot.from_row({**row, **patch})


def test_depth_snapshot_needs_five_levels():
    _, row = parse_message(depth_frame(42), recv_time_ms=1_700_000_000_000)
    snap = DepthSnapshot.from_row(row).model_dump()
    snap["bids"] = snap["bids"][:4]
    with pytest.raises(ValidationError):
        DepthSnapshot(**snap)


def test_feature_tick_round_trip_and_finite_check():
    t = FeatureTick(symbol="BTCUSDT", ts_ms=1, features={"rel_spread": 1e-7})
    assert FeatureTick.model_validate_json(t.model_dump_json()) == t
    for bad in ({}, {"rel_spread": float("nan")}, {"rel_spread": float("-inf")}):
        with pytest.raises(ValidationError):
            FeatureTick(symbol="BTCUSDT", ts_ms=1, features=bad)


def test_stress_prediction_bounds_and_maturity():
    p = StressPrediction(symbol="BTCUSDT", ts_ms=1_000, horizon_s=30, probability=0.7, alert=True,
                         threshold=0.5, model_version="v")
    assert p.matures_at_ms == 31_000 and len(p.prediction_id) == 32
    assert StressPrediction.model_validate_json(p.model_dump_json()) == p
    with pytest.raises(ValidationError):
        StressPrediction(symbol="BTCUSDT", ts_ms=1, horizon_s=30, probability=1.2, alert=True,
                         threshold=0.5, model_version="v")


def test_outcome_label_void_needs_a_reason():
    ok = OutcomeLabel(symbol="BTCUSDT", ts_ms=1, horizon_s=30, label=1)
    assert OutcomeLabel.model_validate_json(ok.model_dump_json()) == ok
    void = OutcomeLabel(symbol="BTCUSDT", ts_ms=1, horizon_s=30, label=None, void_reason="gap")
    assert void.label is None
    with pytest.raises(ValidationError):
        OutcomeLabel(symbol="BTCUSDT", ts_ms=1, horizon_s=30, label=None)
    with pytest.raises(ValidationError):
        OutcomeLabel(symbol="BTCUSDT", ts_ms=1, horizon_s=30, label=0, void_reason="gap")
    with pytest.raises(ValidationError):
        OutcomeLabel(symbol="BTCUSDT", ts_ms=1, horizon_s=30, label=2)


def test_score_request_rejects_unknown_top_level_keys():
    with pytest.raises(ValidationError):
        ScoreRequest(symbol="BTCUSDT", ts_ms=1, features={"a": 1.0}, files={})


def test_json_formatter_emits_extras():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    log = logging.getLogger("t.json")
    log.handlers[:] = [handler]
    log.setLevel(logging.INFO)
    log.info("hello", extra={"symbol": "BTCUSDT", "probability": 0.5})
    row = json.loads(stream.getvalue())
    assert row["msg"] == "hello" and row["symbol"] == "BTCUSDT" and row["level"] == "INFO"

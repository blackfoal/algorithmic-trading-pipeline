from __future__ import annotations

from typing import Any, Dict, Optional

from .base import BaseStrategy, EntryDecision, ExitDecision, OpenPosition


class BollingerMacdStrategy(BaseStrategy):
    """Strategy blueprint for "Bollinger Bands on MACD" guidance-only logic.

    This class DOES NOT compute indicators. It only interprets externally
    supplied signals and returns guidance for the execution/monitoring engine.

    Expected signals/context keys (all booleans unless noted):
      - macd_cross_above_upper_band
      - macd_cross_below_lower_band
      - macd_line_color: one of {"green", "red"}
      - price_move_pct: float (current PnL relative to entry, e.g. -0.031 for -3.1%)

    Entry guidance:
      - Long:  macd_cross_above_upper_band and macd_line_color == "green"
      - Short: macd_cross_below_lower_band and macd_line_color == "red"

    Exit guidance (simple):
      - For a long:  price_move_pct <= -0.03 OR opposite signal (cross below lower & red)
      - For a short: price_move_pct >=  0.03 OR opposite signal (cross above upper & green)
    """

    def evaluate_entry(self, signals: Dict[str, Any]) -> EntryDecision:
        color: Optional[str] = signals.get("macd_line_color")
        if signals.get("macd_cross_above_upper_band") and color == "green":
            return EntryDecision(
                should_enter=True,
                side="long",
                rationale="MACD crossed above upper BB and turned green (trend up)",
                metadata={"source": "bollinger_macd"},
            )

        if signals.get("macd_cross_below_lower_band") and color == "red":
            return EntryDecision(
                should_enter=True,
                side="short",
                rationale="MACD crossed below lower BB and turned red (trend down)",
                metadata={"source": "bollinger_macd"},
            )

        return EntryDecision(should_enter=False, rationale="no_signal", metadata={"source": "bollinger_macd"})

    def evaluate_exit(
        self,
        position: OpenPosition,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> ExitDecision:
        ctx = context or {}
        color: Optional[str] = ctx.get("macd_line_color")
        cross_up: bool = bool(ctx.get("macd_cross_above_upper_band"))
        cross_down: bool = bool(ctx.get("macd_cross_below_lower_band"))
        move_pct: Optional[float] = ctx.get("price_move_pct")  # PnL relative to entry

        if position.side == "long":
            if move_pct is not None and move_pct <= -0.03:
                return ExitDecision(should_exit=True, reason="stop_loss_3pct", metadata={"pnl_pct": move_pct})
            if cross_down and color == "red":
                return ExitDecision(should_exit=True, reason="opposite_signal", metadata={"signal": "downtrend"})
        else:  # short
            if move_pct is not None and move_pct >= 0.03:
                return ExitDecision(should_exit=True, reason="stop_loss_3pct", metadata={"pnl_pct": move_pct})
            if cross_up and color == "green":
                return ExitDecision(should_exit=True, reason="opposite_signal", metadata={"signal": "uptrend"})

        return ExitDecision(should_exit=False, reason="hold")


__all__ = ["BollingerMacdStrategy"]



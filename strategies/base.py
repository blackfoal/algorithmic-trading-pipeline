from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from abc import ABC, abstractmethod


@dataclass
class EntryDecision:
    """Pure characterization of an entry decision.

    No calculations are performed here. Upstream components compute signals and
    pass them to a concrete strategy which returns guidance only.
    """

    should_enter: bool
    side: Optional[str] = None  # "long" or "short"; None when should_enter is False
    rationale: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ExitDecision:
    """Pure characterization of an exit decision.

    The execution/monitoring engine evaluates stops/targets and passes relevant
    context to strategies. Strategies return guidance only.
    """

    should_exit: bool
    reason: str = "hold"  # e.g. "signal", "take_profit", "stop_loss", "expire", "hold"
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class OpenPosition:
    """Minimal container provided to exit rules by the execution engine."""

    side: str  # "long" or "short"
    entry_price: float
    quantity: float


class BaseStrategy(ABC):
    """Minimal strategy interface (no calculations, guidance only).

    - Entry rule: interpret external signals and suggest whether to open a
      position and in which direction.
    - Exit rule: given an open position and external context, suggest whether to
      close it. Stop-loss / take-profit computations are outside this class.
    """

    def __init__(self, name: Optional[str] = None) -> None:
        self.name: str = name or self.__class__.__name__

    # ----- Entry -----
    @abstractmethod
    def evaluate_entry(self, signals: Dict[str, Any]) -> EntryDecision:
        """Return an EntryDecision based solely on external signals."""

    # ----- Exit -----
    @abstractmethod
    def evaluate_exit(
        self,
        position: OpenPosition,
        *,
        context: Optional[Dict[str, Any]] = None,
    ) -> ExitDecision:
        """Return an ExitDecision based on provided context (no computations)."""


__all__ = [
    "BaseStrategy",
    "EntryDecision",
    "ExitDecision",
    "OpenPosition",
]



from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class P2Quantile:
    """
    P² (P-square) streaming quantile estimator.
    Minimal implementation for q in (0,1). Keeps 5 markers.

    This is used only for latency P99 estimation; if state is missing or insufficient,
    it falls back to a small in-memory buffer.
    """
    q: float = 0.99

    # marker positions
    n: list[float] | None = None
    # desired marker positions
    np: list[float] | None = None
    # marker heights
    h: list[float] | None = None
    # desired position increments
    dn: list[float] | None = None

    _init_buf: list[float] | None = None

    def __post_init__(self) -> None:
        if self._init_buf is None:
            self._init_buf = []

    def update(self, x: float) -> None:
        if self.h is None:
            self._init_buf.append(float(x))
            if len(self._init_buf) < 5:
                return
            self._init_buf.sort()
            self.h = [self._init_buf[i] for i in range(5)]
            self.n = [1.0, 2.0, 3.0, 4.0, 5.0]
            self.np = [1.0, 1.0 + 2.0 * self.q, 1.0 + 4.0 * self.q, 3.0 + 2.0 * self.q, 5.0]
            self.dn = [0.0, self.q / 2.0, self.q, (1.0 + self.q) / 2.0, 1.0]
            self._init_buf = None
            return

        assert self.h is not None and self.n is not None and self.np is not None and self.dn is not None

        k = 0
        if x < self.h[0]:
            self.h[0] = float(x)
            k = 0
        elif x < self.h[1]:
            k = 0
        elif x < self.h[2]:
            k = 1
        elif x < self.h[3]:
            k = 2
        elif x <= self.h[4]:
            k = 3
        else:
            self.h[4] = float(x)
            k = 3

        # increment positions
        for i in range(k + 1, 5):
            self.n[i] += 1.0
        for i in range(5):
            self.np[i] += self.dn[i]

        # adjust heights 2..4
        for i in range(1, 4):
            d = self.np[i] - self.n[i]
            if (d >= 1.0 and self.n[i + 1] - self.n[i] > 1.0) or (d <= -1.0 and self.n[i - 1] - self.n[i] < -1.0):
                di = 1.0 if d >= 1.0 else -1.0
                # parabolic prediction
                hp = self._parabolic(i, di)
                if self.h[i - 1] < hp < self.h[i + 1]:
                    self.h[i] = hp
                else:
                    self.h[i] = self._linear(i, di)
                self.n[i] += di

    def _parabolic(self, i: int, d: float) -> float:
        assert self.h is not None and self.n is not None
        n0, n1, n2 = self.n[i - 1], self.n[i], self.n[i + 1]
        h0, h1, h2 = self.h[i - 1], self.h[i], self.h[i + 1]
        return h1 + d / (n2 - n0) * (
            (n1 - n0 + d) * (h2 - h1) / (n2 - n1) + (n2 - n1 - d) * (h1 - h0) / (n1 - n0)
        )

    def _linear(self, i: int, d: float) -> float:
        assert self.h is not None and self.n is not None
        return self.h[i] + d * (self.h[i + int(d)] - self.h[i]) / (self.n[i + int(d)] - self.n[i])

    def value(self) -> float:
        if self.h is None:
            if not self._init_buf:
                return 0.0
            buf = sorted(self._init_buf)
            idx = int(round((len(buf) - 1) * self.q))
            return float(buf[max(0, min(len(buf) - 1, idx))])
        return float(self.h[2])

    def to_state(self) -> dict[str, Any]:
        return {
            "q": float(self.q),
            "n": self.n,
            "np": self.np,
            "h": self.h,
            "dn": self.dn,
            "init_buf": self._init_buf,
        }

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> "P2Quantile":
        q = float(state.get("q", 0.99))
        obj = cls(q=q)
        obj.n = state.get("n")
        obj.np = state.get("np")
        obj.h = state.get("h")
        obj.dn = state.get("dn")
        obj._init_buf = state.get("init_buf")
        if obj._init_buf is None and obj.h is None:
            obj._init_buf = []
        return obj

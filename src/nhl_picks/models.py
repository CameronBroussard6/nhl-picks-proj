from __future__ import annotations
import numpy as np

def poisson_cdf(k: int, mu: float) -> float:
    # returns P(X <= k)
    from math import exp
    s = 0.0
    term = exp(-mu)
    s += term  # k=0
    for i in range(1, k+1):
        term *= mu / i
        s += term
    return min(max(s, 0.0), 1.0)

def prob_at_least(k_plus_half: float, mu: float) -> float:
    # Over k.5 → P(X >= k+1)
    import math
    k = int(math.floor(k_plus_half))
    return 1.0 - poisson_cdf(k, mu)

def fair_odds(p: float) -> float:
    return np.inf if p <= 0 else 1.0 / p

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

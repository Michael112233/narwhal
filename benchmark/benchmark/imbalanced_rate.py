# Copyright(C) Facebook, Inc. and its affiliates.
# This file is used to generate a list of rate with zipf

# Zipf distribution constant

import math
from typing import Optional

class Zipf:
    r: object
    imax: float
    v: float
    q: float
    s: float
    oneminusq: float
    oneminusq_inv: float
    hxm: float
    hx0_minus_hxm: float

    def __init__(self, r: object, s: float, v: float, imax: int):
        if s <= 1.0 or v < 1:
            raise ValueError("s must be > 1.0 and v must be >= 1")
        self.r = r
        self.imax = float(imax)
        self.v = float(v)
        self.q = float(s)
        self.oneminusq = 1.0 - self.q
        self.oneminusq_inv = 1.0 / self.oneminusq
        self.hxm = self.h(self.imax + 0.5)
        self.hx0_minus_hxm = (
            self.h(0.5) - math.exp(math.log(self.v) * (-self.q)) - self.hxm
        )
        self.s = 1 - self.hinv(
            self.h(1.5) - math.exp(-self.q * math.log(self.v + 1.0))
        )

    def h(self, x: float) -> float:
        return math.exp(self.oneminusq * math.log(self.v + x)) * self.oneminusq_inv

    def hinv(self, x: float) -> float:
        return math.exp(self.oneminusq_inv * math.log(self.oneminusq * x)) - self.v

    def uint64(self) -> int:
        k = 0.0
        while True:
            r = self.r.random()  # r in [0, 1)
            ur = self.hxm + r * self.hx0_minus_hxm
            x = self.hinv(ur)
            k = math.floor(x + 0.5)
            if k - x <= self.s:
                break
            if ur >= self.h(k + 0.5) - math.exp(-math.log(k + self.v) * self.q):
                break
        return int(k)


def new_zipf(r: object, s: float, v: float, imax: int) -> Optional[Zipf]:
    if s <= 1.0 or v < 1:
        return None
    return Zipf(r, s, v, imax)


class ZipfAllocator:
    def __init__(self, total_tps: int, nodes: int, s: float, v: float) -> None:
        """
        :param total_tps: total TPS (total load to be allocated)
        :param nodes: number of nodes
        :param s: Zipf distribution parameter s (> 1.0)
        :param v: Zipf distribution parameter v (>= 1)
        """
        if total_tps <= 0:
            raise ValueError("total_tps must be > 0")
        if nodes <= 0:
            raise ValueError("nodes must be > 0")
        if s <= 1.0:
            raise ValueError("s must be > 1.0")
        if v < 1:
            raise ValueError("v must be >= 1")

        self.total_tps = total_tps
        self.nodes = nodes
        self.s = s
        self.v = v

    def allocate(self) -> list[int]:
        weights = [(self.v + k) ** (-self.s) for k in range(1, self.nodes + 1)]
        total_w = sum(weights)
        raw = [self.total_tps * w / total_w for w in weights]
        alloc = [int(x) for x in raw]
        remainder = self.total_tps - sum(alloc)

        if remainder > 0:
            frac = [x - int(x) for x in raw]
            order = sorted(range(self.nodes), key=lambda i: frac[i], reverse=True)
            for i in range(remainder):
                alloc[order[i]] += 1

        return alloc
"""
context_router.py
=================
Core routing logic for CYW_OS v2.1.
Compatible with Python 3.8+
"""
from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple
from uuid import UUID, uuid4
import logging

# Tuning Knobs
MAX_SKEW_NS = 5_000_000_000  # ±5 seconds
MAX_VECTOR_SIZE = 25         # Hard cap
PRUNE_RATIO = 0.2            # Remove bottom 20%

class ClockSkewError(Exception):
    pass

@dataclass
class HLC:
    physical: int
    logical: int
    node_id: UUID

    @staticmethod
    def now(node_id: UUID) -> HLC:
        return HLC(time.monotonic_ns(), 0, node_id)

    def update(self, remote: HLC) -> HLC:
        current_phys = time.monotonic_ns()
        if abs(current_phys - remote.physical) > MAX_SKEW_NS:
            raise ClockSkewError(f"Skew violation")

        phys = max(current_phys, remote.physical, self.physical)
        if phys == self.physical == remote.physical:
            log = max(self.logical, remote.logical) + 1
        else:
            log = 0
        return HLC(phys, log, self.node_id)

@dataclass
class EfficientVectorClock:
    local_node: UUID
    vector: Dict[UUID, int] = field(default_factory=dict)

    def increment(self) -> None:
        self.vector[self.local_node] = self.vector.get(self.local_node, 0) + 1

    def merge(self, other_vector: Dict[UUID, int]) -> None:
        for node, counter in other_vector.items():
            if counter > self.vector.get(node, 0):
                self.vector[node] = counter
        if len(self.vector) > MAX_VECTOR_SIZE:
            self._prune()

    def _prune(self) -> None:
        sorted_nodes = sorted(self.vector.items(), key=lambda item: item[1])
        cutoff = int(MAX_VECTOR_SIZE * PRUNE_RATIO)
        pruned = 0
        for node_id, _ in sorted_nodes:
            if pruned >= cutoff: break
            if node_id != self.local_node:
                del self.vector[node_id]
                pruned += 1

@dataclass
class ContextRouter:
    node_id: UUID = field(default_factory=uuid4)
    hlc: HLC = field(init=False)
    vc: EfficientVectorClock = field(init=False)

    def __post_init__(self):
        self.hlc = HLC.now(self.node_id)
        self.vc = EfficientVectorClock(self.node_id)

    def ingress_packet(self, payload: dict, remote_hlc_tuple: Tuple[int, int, str], remote_vc: dict) -> bool:
        try:
            r_phys, r_log, r_node_str = remote_hlc_tuple
            remote_hlc = HLC(r_phys, r_log, UUID(r_node_str))
            self.hlc = self.hlc.update(remote_hlc)
            self.vc.merge(remote_vc)
            self.vc.increment()
            return True
        except Exception as e:
            logging.error(f"Packet Rejected: {e}")
            return False

if __name__ == "__main__":
    router = ContextRouter()
    print(f"Router Online: {router.node_id}")
    success = router.ingress_packet(
        payload={"data": "ping"},
        remote_hlc_tuple=(time.monotonic_ns(), 0, str(uuid4())),
        remote_vc={}
    )
    print(f"Packet Accepted: {success}")

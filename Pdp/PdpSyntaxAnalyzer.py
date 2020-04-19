from copy import copy, deepcopy
from math import floor
from multiprocessing import Process, Queue
from .PdpStages import PdpProcessor, PdpPipe, PdpFork, PdpBalancingFork, PdpReplicatingFork, PdpJoin

class PdpSyntaxAnalyzer:
    def __init__(self):
        self.forks = []
        self.fork_ptr = 0
        self.inference_flag = False
        self.remainder_flag = False

    def mark_inference(self):
        self.inference_flag = True

    def push_fork(self, fork):
        self.forks.append(fork.splits)
        self.inference_flag = False

    def check_merge(self, join):
        if len(self.forks) == 0 or not self.inference_flag:
            return
        if self.fork_ptr > len(self.forks):
            raise Exception('Ambiguity Error: More join fan in than fork fan out')
        self.forks[self.fork_ptr] -= join.merges
        remainder = self.forks[self.fork_ptr]
        if self.remainder_flag and remainder < 0:
            raise Exception('Ambiguity Error: Partially joining forks')
        while remainder < 0:
            self.fork_ptr += 1
            if self.fork_ptr >= len(self.forks):
                raise Exception('Ambiguity Error: More join fan in than fork fan out')
            remainder += self.forks[self.fork_ptr]

        if remainder > 0:
            if self.forks[self.fork_ptr] < 0:
                raise Exception('Ambiguity Error: Partially joining forks')
            self.remainder_flag = True
        else:
            self.fork_ptr += 1
            self.remainder_flag = False

    def finalize_merge(self):
        if self.fork_ptr < len(self.forks) or self.remainder_flag:
            raise Exception('Ambiguity Error: More fork fan out than join fan in')
        elif self.fork_ptr > len(self.forks):
            raise Exception('Ambiguity Error: More join fan in than fork fan out')
        self.forks = []
        self.fork_ptr = 0
        self.inference_flag = False

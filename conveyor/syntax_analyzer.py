class SyntaxAnalyzer:
    def __init__(self):
        self.forks = []
        self.fork_ptr = 0
        self.implicit_flag = False
        self.remainder_flag = False

    def mark_implicit(self):
        self.implicit_flag = True

    def initialize_fork(self):
        self.forks = []
        self.fork_ptr = 0
        self.implicit_flag = False

    def push_fork(self, fork):
        self.forks.append(fork.splits)

    def check_join(self, join):
        if len(self.forks) == 0 or not self.implicit_flag:
            return
        if self.fork_ptr > len(self.forks):
            raise Exception(
                'Ambiguity Error: More fork fan out than join fan in')
        self.forks[self.fork_ptr] -= join.merges
        remainder = self.forks[self.fork_ptr]
        if self.remainder_flag and remainder < 0:
            raise Exception('Ambiguity Error: Partially joining forks')
        while remainder < 0:
            self.fork_ptr += 1
            if self.fork_ptr >= len(self.forks):
                raise Exception(
                    'Ambiguity Error: More join fan in than fork fan out')
            remainder += self.forks[self.fork_ptr]

        if remainder > 0:
            if self.forks[self.fork_ptr] < 0:
                raise Exception('Ambiguity Error: Partially joining forks')
            self.remainder_flag = True
        else:
            self.fork_ptr += 1
            self.remainder_flag = False

    def finalize_join(self):
        if self.implicit_flag:
            if self.fork_ptr < len(self.forks) or self.remainder_flag:
                raise Exception(
                    'Ambiguity Error: More fork fan out than join fan in')
            elif self.fork_ptr > len(self.forks):
                raise Exception(
                    'Ambiguity Error: More join fan in than fork fan out')
        self.forks = []
        self.fork_ptr = 0
        self.implicit_flag = False

import sys


class PdpStage:
    def __init__(self):
        self.pipe_in = [None]
        self.pipe_out = [None]
        self.next = [None]


class PdpPipe(PdpStage):
    def __init__(self):
        super(PdpPipe, self).__init__()

    def finalize(self):
        while True:
            in_block = self.pipe_in[0].get()
            # If block is "magic value" of none, escape
            if in_block is None:
                break


class PdpProcessor(PdpStage):
    def __init__(self, job):
        super(PdpProcessor, self).__init__()
        self.job = job

    def process(self):
        while True:
            in_block = self.pipe_in[0].get()
            if in_block is None:
                self.pipe_out[0].put(in_block)
                sys.exit()
            out_block = self.job(in_block)
            self.pipe_out[0].put(out_block)


class PdpFork(PdpStage):
    def __init__(self, splits):
        super(PdpFork, self).__init__()
        self.pipe_out = [None] * splits
        self.splits = splits
        self.count = 0

    def fork(self):
        return 0


class PdpReplicatingFork(PdpFork):
    def __init__(self, splits):
        super(PdpReplicatingFork, self).__init__(splits)

    def fork(self):
        while True:
            in_block = self.pipe_in[0].get()
            for i in range(self.splits):
                self.pipe_out[i].put(in_block)
            if in_block is None:
                sys.exit()


class PdpBalancingFork(PdpFork):
    def __init__(self, splits):
        super(PdpBalancingFork, self).__init__(splits)

    def fork(self):
        while True:
            in_block = self.pipe_in[0].get()
            if in_block is None:
                for i in range(self.splits):
                    self.pipe_out[i].put(in_block)
                sys.exit()
            self.pipe_out[self.count].put(in_block)
            self.count = ((self.count + 1) % self.splits)


class PdpJoin(PdpStage):
    def __init__(self, merges):
        super(PdpJoin, self).__init__()
        self.pipe_in = [None] * merges
        self.merges = merges
        self.count = 0
        self.processed = merges
        self.done = [False] * merges

    def merge(self):
        while True:
            in_block = self.pipe_in[self.count].get()
            if in_block is None:
                self.processed = self.processed - 1
                self.done[self.count] = True
                if self.processed == 0:
                    self.pipe_out[0].put(in_block)
                    sys.exit()
            else:
                self.pipe_out[0].put(in_block)

            while self.done[self.count] is True:
                self.count = ((self.count + 1) % self.merges)

import sys


class PdpStep:
    def __init__(self):
        self.pipe_in = [None]
        self.pipe_out = [None]
        self.next = [None]


class PdpPipe(PdpStep):
    def __init__(self):
        super(PdpPipe, self).__init__()

    def finalize(self):
        while True:
            in_block = self.pipe_in[0].get()
            if in_block == None:
                sys.exit()
            print('Final value:', in_block)


class PdpProcessor(PdpStep):
    def __init__(self, job):
        super(PdpProcessor, self).__init__()
        self.job = job

    def process(self):
        while True:
            in_block = self.pipe_in[0].get()
            if in_block == None:
                self.pipe_out[0].put(in_block)
                sys.exit()
            out_block = self.job(in_block)
            self.pipe_out[0].put(out_block)

class PdpFork(PdpStep):
    def __init__(self, splits):
        super(PdpFork, self).__init__()
        self.pipe_out = [None] * splits
        self.splits = splits
        self.count = 0

    def split(self):
        while True:
            in_block = self.pipe_in[0].get()
            if in_block == None:
                sys.exit()
            # TODO: Split into output queues
            self.pipe_out[self.count].put(in_block)
            self.count = ((self.count + 1) % self.splits)

class PdpMerge(PdpStep):
    def __init__(self, merges):
        super(PdpMerge, self).__init__()
        self.pipe_in = [None] * merges
        self.merges = merges
        self.count = 0

    def merge(self):
        while True:
            in_block = self.pipe_in[self.count].get()
            self.count = ((self.count + 1) % self.merges)
            if in_block == None:
                sys.exit()
            # TODO: Split into output queues
            self.pipe_out[0].put(in_block)

import sys


class PdpStep:
    def __init__(self):
        self.pipe_in = None
        self.pipe_out = None
        self.next = None


class PdpPipe(PdpStep):
    def __init__(self):
        super(PdpPipe, self).__init__()
        self.output_processors = []

    def finalize(self):
        while True:
            in_block = self.pipe_in.get()
            if in_block == None:
                sys.exit()
            print('Final value:', in_block)


class PdpProcessor(PdpStep):
    def __init__(self, job):
        super(PdpProcessor, self).__init__()
        self.job = job

    def process(self):
        while True:
            in_block = self.pipe_in.get()
            if in_block == None:
                self.pipe_out.put(in_block)
                sys.exit()
            out_block = self.job(in_block)
            self.pipe_out.put(out_block)

            # todo: add a PDP_Fork class

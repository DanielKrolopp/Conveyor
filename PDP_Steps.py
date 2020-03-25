# Represents a step in the pipeline
class PDP_Step:
    def __init__(self):
        self.pipe_in = None
        self.pipe_out = None
        self.next = None

class PDP_Pipe(PDP_Step):
    def __init__(self):
        super(PDP_Pipe, self).__init__()
        self.output_processors = []

    # I can't think of a better name than process right now
    def process(self):
        pass # todo: in here, we need a producer/consumer queue

class PDP_Processor(PDP_Step):
    def __init__(self, job):
        super(PDP_Processor, self).__init__()
        self.job = job

    def process(self):
        while True:
            in_block = self.pipe_in.recv()
            out_block = self.job(in_block)
            self.pipe_out.send(out_block)

# todo: add a PDP_Fork class

from multiprocessing import Process, Pipe
from PDP_Steps import PDP_Pipe, PDP_Processor

class PDP_Pipeline:
    def __init__(self):

        # Create a dummy PDP_Pipe to start as the head. We should also probably
        # make a PDP_Pipe as the tail too. The dummy head allows us to have
        # something to connect our pipes to. We'll need to add a dummy tail too.
        self.pipeline_tail = PDP_Pipe()
        self.pipeline_head = self.pipeline_tail

    # Add a step to the pipeline. The step can be either a pipe or processor
    def add(self, step):

        # Check for valid steps in the pipeline
        if not isinstance(step, (PDP_Pipe, PDP_Processor)):
            raise Exception('Invalid type! Pipelines must include only PDP_Processors and PDP_Pipes!')

        # A pipeline must start with a processor
        if (not self.pipeline_tail) and not isinstance(step, PDP_Processor):
            raise Exception('A pipeline must start with a processor!')

        # Create a pipe from the existing end of the pipeline to the new step
        pipe_in, pipe_out = Pipe()
        self.pipeline_tail.pipe_out = pipe_in
        step.pipe_in = pipe_out

        # Complete linking the data structure and advance pipeline_tail
        self.pipeline_tail.next = step
        self.pipeline_tail = step

    # Run the pipeline, starting with init_block as the input data object.
    # This traverses the linked-list type structure to create a process for each
    # PDP_Processor/PDP_Pipe.
    def run(self, init_block):

        print('Running...')

        step = self.pipeline_head.next
        while step:
            print('Adding step:', step)
            if isinstance(step, PDP_Processor):
                p = Process(target = step.process)
                p.start()
            elif isinstance(step, PDP_Pipe):
                # Duplicate code. Idk if we're gonna need different functionality
                # at some point
                p = Process(target = step.process)
                p.start()
            else:
                raise Exception('Unknown step in the pipeline!', step)

            step = step.next

        # todo: bootstrap the process by inserting init_block into the head of
        # the pipeline (or forcing the user to be the one to do so, idk).

# Stub code meant for testing
def main():
    def job():
        print('This is a job to be run by a processor')

    pl = PDP_Pipeline()
    proc = PDP_Processor(job)
    pipe = PDP_Pipe()
    pl.add(proc)
    pl.add(pipe)
    pl.run('meh')

if __name__ == '__main__':
    main()

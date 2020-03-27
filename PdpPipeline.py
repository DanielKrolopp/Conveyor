from multiprocessing import Process, Queue
from PdpSteps import PdpPipe, PdpProcessor


class PdpPipeline:
    def __init__(self):

        # Create a dummy PdpPipe to start as the head. We should also probably
        # make a PdpPipe as the tail too. The dummy head allows us to have
        # something to connect our pipes to. We'll need to add a dummy tail too.
        self.num_steps = 0
        self.pipeline_tail = PdpPipe()
        q = Queue()
        self.pipeline_tail.pipe_in = q
        self.pipeline_tail.pipe_out = q
        self.pipeline_head = self.pipeline_tail

    # Add a step to the pipeline. The step can be either a pipe or processor
    def add(self, step):

        # Check for valid steps in the pipeline
        if not isinstance(step, (PdpPipe, PdpProcessor)):
            raise Exception(
                'Invalid type! Pipelines must include only PDP types!')

        # A pipeline must start with a processor
        if (not self.pipeline_tail) and not isinstance(step, PdpProcessor):
            raise Exception('A pipeline must start with a processor!')

        # A PdpProcessor must be preceeded by a PdpPipe
        if isinstance(step, PdpProcessor) and not isinstance(self.pipeline_tail, PdpPipe):
            raise Exception('A PdpProcessor must be preceeded by a PdpPipe!')

        self.num_steps += 1

        if isinstance(step, PdpPipe):
            # Create a pipe from the existing end of the pipeline to the new step
            q = Queue()
            self.pipeline_tail.pipe_out = q
            step.pipe_in = q
            step.pipe_out = q
        elif isinstance(step, PdpProcessor):
            # Link this to the previous pipe
            step.pipe_in = self.pipeline_tail.pipe_out

        # Complete linking the data structure and advance pipeline_tail
        self.pipeline_tail.next = step
        self.pipeline_tail = step

    # Run the pipeline, starting with init_block as the input data object.
    # This traverses the linked-list type structure to create a process for each
    # PdpProcessor/PdpPipe.
    def run(self, init_block_list):

        # Add a dummy pipe at the end for output before running
        self.add(PdpPipe())

        print('Running a', self.num_steps - 1, 'step pipeline...')

        step = self.pipeline_head
        while step:
            if isinstance(step, PdpProcessor):
                p = Process(target=step.process)
                p.start()
            elif isinstance(step, PdpPipe):
                # Display final results
                if step == self.pipeline_tail:
                    p = Process(target=step.finalize)
                    p.start()
            else:
                raise Exception('Unknown step in the pipeline!', step)

            step = step.next

        # todo: bootstrap the process by inserting init_block into the head of
        # the pipeline (or forcing the user to be the one to do so, idk).
        for block in init_block_list:
            self.pipeline_head.pipe_in.put(block)
        self.pipeline_head.pipe_in.put(None)

# Stub code meant for testing

def job(arg):
    print('This is a job to be run by a processor', arg)
    return arg + 1

def example1():
    pl = PdpPipeline()
    pl.add(PdpProcessor(job))
    pl.add(PdpPipe())
    pl.add(PdpProcessor(job))
    pl.run([0, 5, 9])


def main():
    example1()


if __name__ == '__main__':
    main()

from math import floor
from multiprocessing import Process, Queue
from PdpSteps import PdpPipe, PdpProcessor, PdpFork, PdpMerge


class PdpPipeline:
    def __init__(self):

        # Create a dummy PdpPipe to start as the head. We should also probably
        # make a PdpPipe as the tail too. The dummy head allows us to have
        # something to connect our pipes to. We'll need to add a dummy tail too.
        self.num_steps = 0
        self.pipeline_tail = [PdpPipe()]
        q = Queue()
        self.pipeline_tail[0].pipe_in[0] = q
        self.pipeline_tail[0].pipe_out[0] = q
        self.pipeline_head = self.pipeline_tail

    # Add a step to the pipeline. The step can be either a pipe or processor
    def add(self, steps):

        # Check for valid steps in the pipeline
        for i in range(len(steps)):
            step = steps[i]
            if not isinstance(step, (PdpPipe, PdpProcessor, PdpFork, PdpMerge)):
                raise Exception(
                    'Invalid type! Pipelines must include only PDP types!')

            # A pipeline must start with a processor
            if (not self.pipeline_tail) and not isinstance(step, PdpProcessor):
                raise Exception('A pipeline must start with a processor!')

            # A PdpProcessor must be preceeded by a PdpPipe or PdpFork
            if isinstance(step, PdpProcessor) and not isinstance(self.pipeline_tail[0], (PdpPipe, PdpFork, PdpMerge)):
                raise Exception('A PdpProcessor must be preceeded by a PdpPipe, PdpFork, or PdpMerge!')

            # A PdpFork must be preceeded by a PdpPipe
            if isinstance(step, PdpFork) and not isinstance(self.pipeline_tail[0], PdpPipe):
                raise Exception('A PdpFork must be preceeded by a PdpPipe!')

            # A PdpMerge must be preceeded by a PdpPipe
            if isinstance(step, PdpMerge) and not isinstance(self.pipeline_tail[0], PdpPipe):
                raise Exception('A PdpMerge must be preceeded by a PdpPipe!')

            self.num_steps += 1

            if isinstance(step, PdpPipe):
                # Create a pipe from the existing end of the pipeline to the new step
                q = Queue()
                self.pipeline_tail[i].pipe_out[0] = q
                step.pipe_in[0] = q
                step.pipe_out[0] = q
            elif isinstance(step, PdpProcessor):
                # Link this to the previous
                prev_len = len(self.pipeline_tail)
                step_len = len(steps)
                step.pipe_in[0] = self.pipeline_tail[floor(i * prev_len / step_len)].pipe_out[floor(i % (step_len / prev_len))]
            elif isinstance(step, PdpFork):
                # Split input queue into several
                step.pipe_in[0] = self.pipeline_tail[i].pipe_out[0]
                s = [Queue(), Queue(), Queue()]
                step.pipe_out = s
            elif isinstance(step, PdpMerge):
                # Split input queue into several
                for j in range(step.merges):
                    step.pipe_in[j] = self.pipeline_tail[i * step.merges + j].pipe_out[0]
                q = Queue()
                step.pipe_out[0] = q

        # Complete linking the data structure and advance pipeline_tail
        self.pipeline_tail[0].next = steps
        self.pipeline_tail = steps

    # Run the pipeline, starting with init_block as the input data object.
    # This traverses the linked-list type structure to create a process for each
    # PdpProcessor/PdpPipe.
    def run(self, init_block_list):

        # Add a dummy pipe at the end for output before running
        self.add([PdpPipe()])

        print('Running a', self.num_steps - 1, 'step pipeline...')

        steps = self.pipeline_head
        while steps[0]:
            for step in steps:
                if isinstance(step, PdpProcessor):
                    p = Process(target=step.process)
                    p.start()
                elif isinstance(step, PdpPipe):
                    # Display final results
                    if steps == self.pipeline_tail:
                        p = Process(target=step.finalize)
                        p.start()
                elif isinstance(step, PdpFork):
                    # Display final results
                    p = Process(target=step.split)
                    p.start()
                elif isinstance(step, PdpMerge):
                    # Display final results
                    p = Process(target=step.merge)
                    p.start()
                else:
                    raise Exception('Unknown step in the pipeline!', step)

            steps = steps[0].next

        # todo: bootstrap the process by inserting init_block into the head of
        # the pipeline (or forcing the user to be the one to do so, idk).
        for block in init_block_list:
            self.pipeline_head[0].pipe_in[0].put(block)
        self.pipeline_head[0].pipe_in[0].put(None)

# Stub code meant for testing

def job(arg):
    print('This is a job to be run by a processor', arg)
    return arg + 1

def example1():
    pl = PdpPipeline()
    pl.add([PdpProcessor(job)])
    pl.add([PdpPipe()])
    pl.add([PdpFork(3)])
    pl.add([PdpProcessor(job), PdpProcessor(job), PdpProcessor(job)])
    pl.add([PdpPipe(), PdpPipe(), PdpPipe()])
    pl.add([PdpMerge(3)])
    pl.add([PdpProcessor(job)])
    pl.run([0, 5, 9])


def main():
    example1()


if __name__ == '__main__':
    main()

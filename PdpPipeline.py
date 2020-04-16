from copy import copy
from math import floor
from multiprocessing import Process, Queue
from PdpSteps import *


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
    def add(self, step):

        # Check for valid steps in the pipeline
        parallel = []
        print(self.pipeline_tail)
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

        prev_steps = len(self.pipeline_tail)
        prev_pipes = len(self.pipeline_tail[0].pipe_out)
        if isinstance(step, PdpPipe):
            # Create a pipe from the existing end of the pipeline to the new step
            for i in range(prev_steps):
                temp = copy(step)
                q = Queue()
                self.pipeline_tail[i].pipe_out[0] = q
                temp.pipe_in[0] = q
                temp.pipe_out[0] = q
                parallel.append(temp)
        elif isinstance(step, PdpProcessor):
            # Link this to the previous
            for i in range(prev_steps):
                for j in range(prev_pipes):
                    temp = copy(step)
                    temp.pipe_in[0] = self.pipeline_tail[i].pipe_out[j]
                    parallel.append(temp)
        elif isinstance(step, PdpFork):
            # Split input queue into several
            for i in range(prev_steps):
                temp = copy(step)
                temp.pipe_in[0] = self.pipeline_tail[i].pipe_out[0]
                s = [Queue()] * step.splits
                temp.pipe_out = s
                parallel.append(temp)
        elif isinstance(step, PdpMerge):
            # Split input queue into several
            for i in range(floor(prev_steps / step.merges)):
                temp = copy(step)
                for j in range(step.merges):
                    temp.pipe_in[j] = self.pipeline_tail[i * step.merges + j].pipe_out[0]
                q = Queue()
                temp.pipe_out[0] = q
                parallel.append(temp)

        # Complete linking the data structure and advance pipeline_tail
        self.pipeline_tail[0].next = parallel
        self.pipeline_tail = parallel

    # Run the pipeline, starting with init_block as the input data object.
    # This traverses the linked-list type structure to create a process for each
    # PdpProcessor/PdpPipe.
    def run(self, init_block_list):

        # Add a dummy pipe at the end for output before running
        self.add(PdpPipe())

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
                    p = Process(target=step.fork)
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
    pl.add(PdpProcessor(job))
    pl.add(PdpPipe())
    pl.add(PdpBalancingFork(3))
    pl.add(PdpProcessor(job))
    pl.add(PdpPipe())
    pl.add(PdpProcessor(job))
    pl.add(PdpPipe())
    pl.add(PdpMerge(3))
    pl.add(PdpProcessor(job))
    pl.run([0, 5, 9, 7, 2, 1])


def main():
    example1()


if __name__ == '__main__':
    main()

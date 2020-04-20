from copy import copy, deepcopy
from math import floor
from multiprocessing import Process, Queue
from .PdpStages import PdpProcessor, PdpPipe, PdpFork, PdpBalancingFork, PdpReplicatingFork, PdpJoin
from .PdpSyntaxAnalyzer import PdpSyntaxAnalyzer


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
        self.syntax_analyzer = PdpSyntaxAnalyzer()

    # Add a step to the pipeline. The step can be either a pipe or processor
    def add(self, *args):

        # Check for valid steps in the pipeline
        parallel = []
        argc = len(args)
        prev_steps = len(self.pipeline_tail)
        step_ptr = 0
        pipe_ptr = 0
        prev_fanout = 0
        curr_fanin = 0
        for prev in self.pipeline_tail:
            prev_fanout += len(prev.pipe_out)
        if isinstance(args[0], PdpFork):
            self.syntax_analyzer.initialize_fork()
        if isinstance(args[0], PdpJoin):
            for curr in args:
                curr_fanin += curr.merges
        for s in range(len(args)):
            step = args[s]
            if not isinstance(step, (PdpPipe, PdpProcessor, PdpFork, PdpJoin)):
                raise Exception(
                    'Invalid type! Pipelines must include only PDP types!')

            # A pipeline must start with a processor
            if (not self.pipeline_tail) and not isinstance(step, PdpProcessor):
                raise Exception('A pipeline must start with a processor!')

            # No need to iterate through more than one PdpPipe
            if isinstance(step, PdpPipe) and (s > 0):
                break

            # A PdpProcessor must be preceded by a PdpPipe or PdpFork
            if isinstance(step, PdpProcessor) and not isinstance(self.pipeline_tail[0], (PdpPipe, PdpFork, PdpJoin)):
                self.add(PdpPipe())
                print("Warning: Adding an implicit pipe between a PdpProcessor and PdpFork/PdpJoin")

            # A PdpFork must be preceded by a PdpPipe or PdpProcessor
            if isinstance(step, PdpFork) and not isinstance(self.pipeline_tail[0], (PdpProcessor, PdpPipe)):
                raise Exception('A PdpFork must be preceded by a PdpProcessor or PdpPipe!')

            # A PdpJoin must be preceded by a PdpPipe or PdpProcessor
            if isinstance(step, PdpJoin) and not isinstance(self.pipeline_tail[0], (PdpProcessor, PdpPipe)):
                raise Exception('A PdpJoin must be preceded by a PdpProcessor or PdpPipe!')

            self.num_steps += 1

            if isinstance(step, PdpPipe):
                # Check if the previous step is a pipe. If it is, skip the pipe
                if isinstance(self.pipeline_tail, PdpPipe):
                    print('Warning: Sequential pipes detected. Coalescing into one pipe')
                    continue
                # Create a pipe from the existing end of the pipeline to the new step
                for i in range(int(prev_steps)):
                    prev_pipes = len(self.pipeline_tail[i].pipe_out)
                    for j in range(prev_pipes):
                        temp = deepcopy(step)
                        q = Queue()
                        self.pipeline_tail[i].pipe_out[j] = q
                        temp.pipe_in[0] = q
                        temp.pipe_out[0] = q
                        parallel.append(temp)

            elif isinstance(step, PdpProcessor):
                # Ensure job is a real function
                if not callable(step.job):
                    raise Exception('Invalid type! Pipeline processors must have a valid job!')

                # If number of objects in previous step can be evenly divided into job groups, do that
                if prev_steps % argc == 0:
                    for i in range(int(prev_steps / argc)):
                        prev_pipes = len(self.pipeline_tail[step_ptr + i].pipe_out)
                        for j in range(prev_pipes):
                            temp = deepcopy(step)
                            temp.pipe_in[0] = self.pipeline_tail[step_ptr + i].pipe_out[j]
                            parallel.append(temp)
                    step_ptr += int(prev_steps / argc)
                # Else if enough jobs were given to explicitly assign one job to each pipe output, do that
                elif prev_fanout == argc:
                    temp = deepcopy(step)
                    temp.pipe_in[0] = self.pipeline_tail[step_ptr].pipe_out[pipe_ptr]
                    parallel.append(temp)
                    if pipe_ptr + 1 == len(self.pipeline_tail[step_ptr].pipe_out):
                        step_ptr += 1
                        pipe_ptr = 0
                    else:
                        pipe_ptr += 1
                # Else we have ambiguity on how to divide the jobs (might be able to make some inferences)
                else:
                    raise Exception('Ambiguity Error: Jobs cannot be divided among fanout of previous step')

            elif isinstance(step, PdpFork):
                # If number of objects in previous step can be evenly divided into forking groups, do that
                if prev_steps % argc == 0:
                    for i in range(int(prev_steps / argc)):
                        # Split input queue into several
                        temp = deepcopy(step)
                        q = Queue()
                        self.pipeline_tail[step_ptr + i].pipe_out[0] = q
                        temp.pipe_in[0] = q
                        s = []
                        for j in range(step.splits):
                            s.append(Queue())
                        temp.pipe_out = s
                        parallel.append(temp)
                        self.syntax_analyzer.push_fork(temp)
                    step_ptr += int(prev_steps / argc)
                # Else we have ambiguity on how to allocate forks
                else:
                    raise Exception('Ambiguity Error: Forks cannot be divided among fanout of previous step')
            elif isinstance(step, PdpJoin):
                # If fanout of previous step can be evenly divided into merge groups, do that
                if prev_fanout % curr_fanin == 0:
                    for i in range(int(prev_fanout / curr_fanin)):
                        # Merge several input queues into one
                        temp = deepcopy(step)
                        m = [Queue()] * step.merges
                        temp.pipe_in = m
                        for j in range(step.merges):
                            self.pipeline_tail[step_ptr + i * step.merges + j].pipe_out[0] = temp.pipe_in[j]
                        q = Queue()
                        temp.pipe_out[0] = q
                        parallel.append(temp)
                        self.syntax_analyzer.check_join(temp)
                    step_ptr += int(step.merges * prev_fanout / curr_fanin)
                # Else we have ambiguity on how to allocate merges
                else:
                    raise Exception('Ambiguity Error: Merges cannot be divided among fanout of previous step')

        if (isinstance(args[0], PdpProcessor) and argc == prev_fanout) or isinstance(args[0], PdpPipe):
            self.syntax_analyzer.mark_explicit()

        if isinstance(args[0], PdpJoin):
            self.syntax_analyzer.finalize_join()

        # Complete linking the data structure and advance pipeline_tail
        self.pipeline_tail[0].next = parallel
        self.pipeline_tail = parallel

    # Run the pipeline, starting with init_block as the input data object.
    # This traverses the linked-list type structure to create a process for each
    # PdpProcessor/PdpPipe.
    def run(self, init_block_list):

        # Add a dummy pipe at the end for output before running if not already added
        if not isinstance(self.pipeline_tail[0], PdpPipe):
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
                elif isinstance(step, PdpJoin):
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
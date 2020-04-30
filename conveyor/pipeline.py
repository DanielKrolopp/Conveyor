from copy import deepcopy
from multiprocessing import Process, Queue
from .stages import Processor, Pipe, _Fork, ReplicatingFork, Join
from .syntax_analyzer import SyntaxAnalyzer
from . import common_memory

class Pipeline:
    def __init__(self, shared_memory_amt=0):

        # Create a dummy Pipe to start as the head. We should also probably
        # make a Pipe as the tail too. The dummy head allows us to have
        # something to connect our pipes to. We'll need to add a dummy tail too.
        self.num_stages = 0
        self.pipeline_tail = [Pipe()]
        q = Queue()
        self.pipeline_tail[0].pipe_in[0] = q
        self.pipeline_tail[0].pipe_out[0] = q
        self.pipeline_head = self.pipeline_tail
        self.syntax_analyzer = SyntaxAnalyzer()
        self.active_fork = None

        # initiate shared memory if it is requested
        if shared_memory_amt > 0:
            # Ensure we are using python 3.8+ which has shared memory
            try:
                from multiprocessing import shared_memory
            except ImportError:
                raise Exception('Multiprocessing shared_memory module is not available. Are you using Python 3.8+ ?')
            else:
                global common_memory
                shared_memory_handle = shared_memory.SharedMemory(name=common_memory, create=True, size=shared_memory_amt)
                shared_memory_handle.close()



    # Add a stage to the pipeline. The stage can be either a pipe or processor
    def add(self, *args):

        argc = len(args)

        if argc == 0:
            raise Exception('Invalid arguments! Function requires non-empty input!')

        # Check for valid stages in the pipeline
        if not isinstance(args[0], (Pipe, Processor, _Fork, Join)):
            raise Exception('Invalid type! Pipelines must include only Conveyor types!')

        # Check for valid stages in the pipeline
        mixed_step = False
        new_args = []
        step_arg = args[0]
        for i in range(len(args)):
            arg = args[i]
            if not isinstance(arg, type(step_arg)):
                if isinstance(step_arg, Pipe):
                    step_arg = arg
                    if isinstance(step_arg, Processor):
                        mixed_step = True
                        new_args.append(arg)
                    elif isinstance(step_arg, _Fork):
                        new_args[0] = ReplicatingFork(1)
                        new_args.append(arg)
                    elif isinstance(step_arg, Join):
                        new_args[0] = Join(1)
                        new_args.append(arg)

                elif isinstance(arg, Pipe):
                    if isinstance(step_arg, Processor):
                        mixed_step = True
                        new_args.append(Pipe())
                    elif isinstance(step_arg, _Fork):
                        new_args.append(ReplicatingFork(1))
                    elif isinstance(step_arg, Join):
                        new_args.append(Join(1))

                else:
                    raise Exception('Invalid types! All non Pipe objects in stage must be in same subclass')
            else:
                new_args.append(arg)
        args = new_args
        # A pipeline cannot start with a Join (nothing to join to!)
        if self.num_stages < 1 and isinstance(args[0], Join):
            raise Exception('A pipeline cannot start with a Join (nothing to join to!)')

        # All Conveyor objects besides Pipe must be preceded by a Pipe
        if not isinstance(step_arg, Pipe):
            for prev_step in self.pipeline_tail:
                if not isinstance(prev_step, Pipe):
                    self.add(Pipe())
                    break

        parallel = []
        prev_stages = len(self.pipeline_tail)
        stage_ptr = 0
        pipe_ptr = 0
        fork_ptr = 0
        prev_fanout = 0
        curr_fanin = 0

        for prev in self.pipeline_tail:
            prev_fanout += len(prev.pipe_out)

        for curr in args:
            curr_fanin += len(curr.pipe_in)

        if isinstance(args[0], _Fork):
            self.syntax_analyzer.initialize_fork()

        for s in range(len(args)):
            stage = args[s]

            # No need to iterate through more than one Pipe if only pipes in this step
            if isinstance(stage, Pipe) and not mixed_step and (s > 0):
                break

            self.num_stages += 1

            if isinstance(stage, Pipe):
                # Create a pipe from the existing end of the pipeline to the new stage
                if not mixed_step:
                    for i in range(int(prev_stages)):
                        prev_pipes = len(self.pipeline_tail[i].pipe_out)
                        for j in range(prev_pipes):
                            temp = deepcopy(stage)
                            q = Queue()
                            # If previous step is a pipe, use its queue instead
                            if isinstance(self.pipeline_tail[i], Pipe):
                                q = self.pipeline_tail[i].pipe_out[j]
                            else:
                                self.pipeline_tail[i].pipe_out[j] = q
                            temp.pipe_in[0] = q
                            temp.pipe_out[0] = q
                            parallel.append(temp)

                elif prev_fanout == curr_fanin:
                    temp = deepcopy(stage)
                    q = Queue()
                    # If previous step is a pipe, use its queue instead
                    if isinstance(self.pipeline_tail[stage_ptr], Pipe):
                        q = self.pipeline_tail[stage_ptr].pipe_out[pipe_ptr]
                    else:
                        self.pipeline_tail[stage_ptr].pipe_out[pipe_ptr] = q
                    temp.pipe_in[0] = q
                    temp.pipe_out[0] = q
                    parallel.append(temp)
                    if pipe_ptr + 1 == len(self.pipeline_tail[stage_ptr].pipe_out):
                        stage_ptr += 1
                        pipe_ptr = 0
                    else:
                        pipe_ptr += 1

                # If number of objects in previous stage can be evenly divided into pipe groups, do that
                elif self.active_fork is None and prev_stages % curr_fanin == 0:
                    for i in range(int(prev_stages / curr_fanin)):
                        temp = deepcopy(stage)
                        q = Queue()
                        # If previous step is a pipe, use its queue instead
                        if isinstance(self.pipeline_tail[stage_ptr + i], Pipe):
                            q = self.pipeline_tail[stage_ptr + i].pipe_out[0]
                        else:
                            self.pipeline_tail[stage_ptr + i].pipe_out[0] = q
                        temp.pipe_in[0] = q
                        temp.pipe_out[0] = q
                        parallel.append(temp)
                    stage_ptr += int(prev_stages / curr_fanin)

                # If there was a fork previously in the pipeline with no join between it and this stage,
                # split pipes based on fork
                elif self.active_fork is not None and len(self.active_fork) % curr_fanin == 0:
                    prev_forks = len(self.active_fork)
                    index = stage_ptr
                    for i in range(int(prev_forks / curr_fanin)):
                        prev_pipes = len(self.active_fork[fork_ptr].pipe_out)
                        fork_ptr += 1
                        for j in range(prev_pipes):
                            temp = deepcopy(stage)
                            q = Queue()
                            # If previous step is a pipe, use its queue instead
                            if isinstance(self.pipeline_tail[index], Pipe):
                                q = self.pipeline_tail[index].pipe_out[0]
                            else:
                                self.pipeline_tail[index].pipe_out[0] = q
                            temp.pipe_in[0] = q
                            temp.pipe_out[0] = q
                            parallel.append(temp)
                            index += 1
                    stage_ptr = index
                # Else we have ambiguity on how to divide the pipes
                else:
                    raise Exception('Ambiguity Error: Pipes cannot be divided among fanout of previous stage')

            elif isinstance(stage, Processor):
                # Ensure job is a real function
                if not callable(stage.job):
                    raise Exception('Invalid type! Pipeline processors must have a valid job!')

                # Else if enough jobs were given to explicitly assign one job to each pipe output, do that
                if prev_fanout == curr_fanin:
                    temp = deepcopy(stage)
                    temp.pipe_in[0] = self.pipeline_tail[stage_ptr].pipe_out[pipe_ptr]
                    parallel.append(temp)
                    if pipe_ptr + 1 == len(self.pipeline_tail[stage_ptr].pipe_out):
                        stage_ptr += 1
                        pipe_ptr = 0
                    else:
                        pipe_ptr += 1

                # If number of objects in previous stage can be evenly divided into job groups, do that
                elif self.active_fork is None and prev_stages % curr_fanin == 0:
                    for i in range(int(prev_stages / curr_fanin)):
                        temp = deepcopy(stage)
                        temp.pipe_in[0] = self.pipeline_tail[stage_ptr + i].pipe_out[0]
                        parallel.append(temp)
                    stage_ptr += int(prev_stages / curr_fanin)

                # If there was a fork previously in the pipeline with no join between it and this stage,
                # split jobs based on fork
                elif self.active_fork is not None and len(self.active_fork) % curr_fanin == 0:
                    prev_forks = len(self.active_fork)
                    index = stage_ptr
                    for i in range(int(prev_forks / curr_fanin)):
                        prev_pipes = len(self.active_fork[fork_ptr].pipe_out)
                        fork_ptr += 1
                        for j in range(prev_pipes):
                            temp = deepcopy(stage)
                            temp.pipe_in[0] = self.pipeline_tail[index].pipe_out[0]
                            parallel.append(temp)
                            index += 1
                    stage_ptr = index
                # Else we have ambiguity on how to divide the jobs (might be able to make some inferences)
                else:
                    raise Exception('Ambiguity Error: Jobs cannot be divided among fanout of previous stage')

            elif isinstance(stage, _Fork):
                # If number of objects in previous stage can be evenly divided into forking groups, do that
                if prev_stages % curr_fanin == 0:
                    for i in range(int(prev_stages / curr_fanin)):
                        # Split input queue into several
                        temp = deepcopy(stage)
                        temp.pipe_in[0] = self.pipeline_tail[stage_ptr + i].pipe_out[0]
                        parallel.append(temp)
                        self.syntax_analyzer.push_fork(temp)
                    stage_ptr += int(prev_stages / curr_fanin)
                # Else we have ambiguity on how to allocate forks
                else:
                    raise Exception('Ambiguity Error: Forks cannot be divided among fanout of previous stage')
            elif isinstance(stage, Join):
                # If fanout of previous stage can be evenly divided into merge groups, do that
                if prev_fanout % curr_fanin == 0:
                    for i in range(int(prev_fanout / curr_fanin)):
                        # Merge several input queues into one
                        temp = deepcopy(stage)
                        for j in range(stage.merges):
                            temp.pipe_in[j] = self.pipeline_tail[stage_ptr + i * stage.merges + j].pipe_out[0]
                        parallel.append(temp)
                        self.syntax_analyzer.check_join(temp)
                    stage_ptr += int(stage.merges * prev_fanout / curr_fanin)
                # Else we have ambiguity on how to allocate merges
                else:
                    raise Exception('Ambiguity Error: Merges cannot be divided among fanout of previous stage')

        if isinstance(args[0], Processor) and curr_fanin != prev_fanout:
            self.syntax_analyzer.mark_implicit()

        if isinstance(args[0], _Fork):
            self.active_fork = parallel

        if isinstance(args[0], Join):
            self.syntax_analyzer.finalize_join()
            self.active_fork = None

        # Complete linking the data structure and advance pipeline_tail
        self.pipeline_tail[0].next = parallel
        self.pipeline_tail = parallel

    # Run the pipeline, starting with init_block as the input data object.
    # This traverses the linked-list type structure to create a process for each
    # Processor/Pipe.
    def run(self, init_block_list):

        # Add a dummy pipe at the end for output before running if not already added
        if not isinstance(self.pipeline_tail[0], Pipe):
            self.add(Pipe())

        stages = self.pipeline_head
        while stages[0]:
            for stage in stages:
                if isinstance(stage, Processor):
                    p = Process(target=stage.process)
                    p.start()
                elif isinstance(stage, Pipe):
                    # Display final results
                    if stages == self.pipeline_tail:
                        p = Process(target=stage.finalize)
                        p.start()
                elif isinstance(stage, _Fork):
                    # Display final results
                    p = Process(target=stage.fork)
                    p.start()
                elif isinstance(stage, Join):
                    # Display final results
                    p = Process(target=stage.merge)
                    p.start()
                else:
                    raise Exception('Unknown stage in the pipeline!', stage)

            stages = stages[0].next

        # todo: bootstrap the process by inserting init_block into the head of
        # the pipeline (or forcing the user to be the one to do so, idk).
        for block in init_block_list:
            self.pipeline_head[0].pipe_in[0].put(block)
        self.pipeline_head[0].pipe_in[0].put(None)

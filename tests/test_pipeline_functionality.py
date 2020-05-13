from unittest import TestCase
from collections import Counter
from threading import Lock

from conveyor.pipeline import Pipeline
from conveyor.stages import Pipe, Processor, BalancingFork, ReplicatingFork, Join
from . import dummy_return_arg


'''
Test the pipeline's behavior. Input/output checking.
'''


class TestPipelineFunctionality(TestCase):

    '''
    The most basic test. Create a pipeline, push in one value and retrieve it.
    '''

    def test_pipeline_basic(self):
        def finalize(arg):
            self.assertEqual(arg, 3)

        pl = Pipeline()
        pl.add(Processor(finalize))
        pl.run([3])

    '''
    Make sure that Processor -> Pipe -> Processor works.
    '''

    def test_pipeline_single_processor(self):
        def job(arg):
            return arg + 1

        def finalize(arg):
            self.assertEqual(arg, 4)

        pl = Pipeline()
        pl.add(Processor(job))
        pl.add(Pipe())
        pl.add(Processor(finalize))
        pl.run([3])

    '''
    Test balancing forks---make sure that the same number of jobs are assigned to each side.
    '''

    def test_balancing_forks(self):
        self.counts = Counter()

        def job1(arg):
            return 'job1'

        def job2(arg):
            return 'job2'

        def finalize(arg):
            self.counts[arg] += 1

        pl = Pipeline()
        pl.add(BalancingFork(2))
        pl.add(Processor(job1), Processor(job2))
        pl.add(Join(2))
        pl.add(Processor(finalize))
        pl.run([False, False])

        self.assertEqual(self.counts['job1'], self.counts['job2'])

    '''
    Test replicating forks---make sure that the same number of jobs are assigned to each side.
    '''
    def test_replicating_forks(self):
        self.counts = Counter()

        def job1(arg):
            return 'job1'

        def job2(arg):
            return 'job2'

        def finalize(arg):
            self.counts[arg] += 1

        pl = Pipeline()
        pl.add(BalancingFork(2))
        pl.add(Processor(job1), Processor(job2))
        pl.add(Join(2))
        pl.add(Processor(finalize))
        pl.run([False, False])
        print(pl)

        self.assertEqual(self.counts['job1'], self.counts['job2'])

    '''
    Allow a pipeline to run multiple times without error.
    This should not hang on the second run.
    '''

    def test_allow_multiple_pipeline_runs(self):

        def job(arg):
            if arg == 'second':
                self.lock.release()


        self.lock = Lock()
        pl = Pipeline()

        pl.add(Processor(dummy_return_arg))
        pl.add(Pipe())
        pl.add(Processor(dummy_return_arg))
        pl.add(Pipe())
        pl.add(Processor(job))
        self.lock.acquire()
        pl.run(['first'])
        pl.run(['second'])
        self.lock.acquire(blocking=False)
        self.assertTrue(self.lock.locked(), 'The second pipeline run was not successful')

    '''
    Test forks and joins. 4 messages should come through. 3 of them are
    manipulated along the way.
    '''

    def test_fork_and_join1(self):
        self.counts = Counter()

        def count(arg):
            _, string = arg
            self.counts[string] += 1
            if len(self.counts) == 4:
                self.assertEqual(self.counts['ttring'], 1)
                self.assertEqual(self.counts['turing'], 1)
                self.assertEqual(self.counts['suring'], 1)
                self.assertEqual(self.counts['string'], 1)

        def dont_manipulate(arg):
            stage, string = arg
            return (stage + 1, string)

        def manipulate(arg):
            stage, string = arg
            l = list(string)
            l[stage] = chr(ord(l[stage]) + 1)
            return (stage + 1, ''.join(l))

        pl = Pipeline()
        pl.add(BalancingFork(2))
        pl.add(Pipe())
        pl.add(Processor(manipulate),
               Processor(dont_manipulate))
        pl.add(ReplicatingFork(2))
        pl.add(Processor(manipulate), Processor(dont_manipulate),
             Processor(manipulate), Processor(dont_manipulate))
        pl.add(Join(4))
        pl.add(Processor(count))
        pl.run([(0, 'string'), (0, 'string')])

    '''
    Test open/close paradigm. Should only call Pipeline.open() and
    Pipeline.close() once each.
    '''

    def test_open_close(self):
        from math import sqrt

        def square_root(arg):
            return sqrt(arg)

        def cube(arg):
            return arg ** 3

        pl = Pipeline()
        pl.add(ReplicatingFork(2))
        pl.add(Processor(square_root), Processor(cube))
        pl.add(Processor(square_root), Pipe())
        pl.add(Pipe())
        pl.add(Join(2))
        pl.add(Processor(print))
        self.assertTrue(pl.closed, 'Pipeline should be closed')

        with pl as pipeline:
            self.assertTrue(pipeline.opened, 'Pipeline should be opened')
            pipeline.run([1, 2])
            self.assertTrue(pipeline.opened, 'Pipeline should be opened')
            pipeline.run([1, 2])
            self.assertTrue(pipeline.opened, 'Pipeline should be opened')

        self.assertTrue(pl.closed)

    '''
    When calling .run() without the surrounding `with` statement, the Pipeline
    should automatically open and close.
    '''

    def test_automatic_open_close(self):
        from math import sqrt

        def square_root(arg):
            return sqrt(arg)

        def cube(arg):
            return arg ** 3

        pl = Pipeline()
        pl.add(ReplicatingFork(2))
        pl.add(Processor(square_root), Processor(cube))
        pl.add(Processor(square_root), Pipe())
        pl.add(Pipe())
        pl.add(Join(2))
        pl.add(Processor(print))
        self.assertTrue(pl.closed, 'Pipeline should be closed')
        pl.run([2, 7, 9])
        self.assertTrue(pl.closed, 'Pipeline should be closed')

from unittest import TestCase

from conveyor.pipeline import Pipeline
from conveyor.stages import Processor, Pipe, _Fork, ReplicatingFork, BalancingFork, Join

from collections import Counter
from . import dummy_return_arg

'''
Test configurations that are/aren't allowed in the pipeline.
'''


class TestPipelineArchitecture(TestCase):
    '''
    Allow only valid pipeline elements
    '''

    def test_pipeline_valid_type(self):
        def finalize(arg):
            return True

        pl = Pipeline()

        # String
        with self.assertRaises(Exception) as e:
            pl.add("cookie")
        self.assertEqual(
            str(e.exception), 'Invalid type! Pipelines must include only Conveyor types!')

        # List
        with self.assertRaises(Exception) as e:
            pl.add([])
        self.assertEqual(
            str(e.exception), 'Invalid type! Pipelines must include only Conveyor types!')

        # Valid processor
        try:
            pl.add(Processor(finalize))
        except Exception:
            self.fail('Should not raise an exception: ' + str(e))

    '''
    Allow starting with valid pipeline stages
    '''

    def test_pipeline_valid_start(self):
        def finalize(arg):
            return True

        pl = Pipeline()

        # Join (should not be allowed at pipeline head)
        with self.assertRaises(Exception) as e:
            pl.add(Join(2))
        self.assertEqual(
            str(e.exception), 'A pipeline cannot start with a Join (nothing to join to!)')

    '''
    Do not allow ambiguity in inferring how many jobs of each type to create
    '''

    def test_pipeline_ambiguity(self):
        def job1(arg):
            return 'job1'

        def job2(arg):
            return 'job2'
        pl = Pipeline()
        pl.add(Processor(job1))
        pl.add(ReplicatingFork(2))
        pl.add(Processor(job1), Processor(job2))
        pl.add(ReplicatingFork(4),
               ReplicatingFork(2))
        pl.add(Processor(job2), Processor(job1))
        with self.assertRaises(Exception) as e:
            pl.add(Join(2), Join(4))
        self.assertEqual(
            str(e.exception), 'Ambiguity Error: Partially joining forks')

    '''
    Create a fork and then too many processors to handle
    '''

    def test_too_many_processors(self):
        pl = Pipeline()
        pl.add(ReplicatingFork(2))

        with self.assertRaises(Exception) as e:
            pl.add(Processor(dummy_return_arg), Processor(
                dummy_return_arg), Processor(dummy_return_arg))
        self.assertEqual(
            str(e.exception), 'Ambiguity Error: Jobs cannot be divided among fanout of previous stage')

    '''
    Processors and pipes should be allowed to mix
    '''

    def test_processor_pipe_mix(self):
        self.counts = Counter()

        def count(arg):
            _, string = arg
            self.counts[string] += 1
            if len(self.counts) == 3:
                self.assertEqual(self.counts['ttring'], 1)
                self.assertEqual(self.counts['string'], 2)

        def manipulate(arg):
            stage, string = arg
            l = list(string)
            l[stage] = chr(ord(l[stage]) + 1)
            return (stage + 1, ''.join(l))

        pl = Pipeline()
        pl.add(ReplicatingFork(3))
        pl.add(Processor(manipulate),
               Pipe(), Pipe())
        pl.add(Join(3))
        pl.add(Processor(count))
        pl.run([(0, 'string')])

    '''
    Forks and pipes should be allowed to mix
    '''

    def test_fork_pipe_mix(self):
        self.counts = Counter()

        def count(arg):
            _, string = arg
            self.counts[string] += 1
            if len(self.counts) == 4:
                self.assertEqual(self.counts['ttring'], 2)
                self.assertEqual(self.counts['string'], 2)

        def manipulate(arg):
            stage, string = arg
            l = list(string)
            l[stage] = chr(ord(l[stage]) + 1)
            return (stage + 1, ''.join(l))

        def dont_manipulate(arg):
            stage, string = arg
            return (stage + 1, string)

        pl = Pipeline()
        pl.add(ReplicatingFork(2))
        pl.add(Pipe(), BalancingFork(2))
        pl.add(Processor(dont_manipulate),
               Processor(manipulate),
               Processor(manipulate))
        pl.add(Join(3))
        pl.add(Processor(count))
        pl.run([(0, 'string'), (0, 'string')])

    '''
    Forks and joins should be allowed to mix
    '''

    def test_join_pipe_mix(self):
        self.counts = Counter()

        def count(arg):
            _, string = arg
            self.counts[string] += 1
            if len(self.counts) == 3:
                self.assertEqual(self.counts['string'], 1)
                self.assertEqual(self.counts['suring'], 1)
                self.assertEqual(self.counts['turing'], 1)

        def manipulate(arg):
            stage, string = arg
            l = list(string)
            l[stage] = chr(ord(l[stage]) + 1)
            return (stage + 1, ''.join(l))

        def dont_manipulate(arg):
            stage, string = arg
            return (stage + 1, string)

        pl = Pipeline()
        pl.add(ReplicatingFork(3))
        pl.add(Processor(dont_manipulate),
               Processor(dont_manipulate),
               Processor(manipulate))
        pl.add(Pipe(), Join(2))
        pl.add(Processor(dont_manipulate),
               Processor(manipulate))
        pl.add(Join(2))
        pl.add(Processor(count))
        pl.run([(0, 'string')])

    '''
    Processors and forks should not be allowed to mix
    '''

    def test_processor_fork_mix(self):
        def job(arg):
            return 'job'

        pl = Pipeline()
        pl.add(ReplicatingFork(2))
        with self.assertRaises(Exception) as e:
            pl.add(Processor(job), BalancingFork(2))
        self.assertEqual(
            str(e.exception), 'Invalid types! All non Pipe objects in stage must be in same subclass')

    '''
    Processors and joins should not be allowed to mix
    '''

    def test_processor_join_mix(self):
        def job(arg):
            return 'job'

        pl = Pipeline()
        pl.add(ReplicatingFork(3))
        with self.assertRaises(Exception) as e:
            pl.add(Processor(job), Join(2))
        self.assertEqual(
            str(e.exception), 'Invalid types! All non Pipe objects in stage must be in same subclass')

    '''
    Forks and joins should not be allowed to mix
    '''

    def test_fork_join_mix(self):
        pl = Pipeline()
        pl.add(ReplicatingFork(3))
        with self.assertRaises(Exception) as e:
            pl.add(Join(2), BalancingFork(2))
        self.assertEqual(
            str(e.exception), 'Invalid types! All non Pipe objects in stage must be in same subclass')

    '''
    Disallow end users from trying to use _Fork abstract class
    '''

    def test_disallow_abstract_Fork(self):
        pl = Pipeline()
        with self.assertRaises(Exception) as e:
            pl.add(_Fork(2))
        self.assertEqual(
            str(e.exception), '_Fork is an abstract class. Use ReplicatingFork or BalancingFork instead.')

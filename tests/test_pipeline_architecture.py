from unittest import TestCase

from Pdp import PdpPipeline
from Pdp import PdpStages

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

        pl = PdpPipeline.PdpPipeline()

        # String
        with self.assertRaises(Exception) as e:
            pl.add("cookie")
        self.assertEqual(
            str(e.exception), 'Invalid type! Pipelines must include only PDP types!')

        # List
        with self.assertRaises(Exception) as e:
            pl.add([])
        self.assertEqual(
            str(e.exception), 'Invalid type! Pipelines must include only PDP types!')

        # Valid processor
        try:
            pl.add(PdpStages.PdpProcessor(finalize))
        except Exception:
            self.fail('Should not raise an exception: ' + str(e))

    '''
    Allow starting with valid pipeline stages
    '''

    def test_pipeline_valid_start(self):
        def finalize(arg):
            return True

        pl = PdpPipeline.PdpPipeline()

        # Join (should not be allowed at pipeline head)
        with self.assertRaises(Exception) as e:
            pl.add(PdpStages.PdpJoin(2))
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
        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpProcessor(job1))
        pl.add(PdpStages.PdpReplicatingFork(2))
        pl.add(PdpStages.PdpProcessor(job1), PdpStages.PdpProcessor(job2))
        pl.add(PdpStages.PdpReplicatingFork(4),
               PdpStages.PdpReplicatingFork(2))
        pl.add(PdpStages.PdpProcessor(job2), PdpStages.PdpProcessor(job1))
        with self.assertRaises(Exception) as e:
            pl.add(PdpStages.PdpJoin(2), PdpStages.PdpJoin(4))
        self.assertEqual(
            str(e.exception), 'Ambiguity Error: Partially joining forks')

    '''
    Create a fork and then too many processors to handle
    '''

    def test_too_many_processors(self):
        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpReplicatingFork(2))

        with self.assertRaises(Exception) as e:
            pl.add(PdpStages.PdpProcessor(dummy_return_arg), PdpStages.PdpProcessor(
                dummy_return_arg), PdpStages.PdpProcessor(dummy_return_arg))
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
            if len(self.counts) == 2:
                self.assertEqual(self.counts['ttring'], 1)
                if self.counts['string'] != 2:
                    self.fail("Counted " + str(self.counts['string']) + " instances of string, expected 2")

        def manipulate(arg):
            stage, string = arg
            l = list(string)
            l[stage] = chr(ord(l[stage]) + 1)
            return (stage + 1, ''.join(l))

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpReplicatingFork(3))
        pl.add(PdpStages.PdpProcessor(manipulate),
               PdpStages.PdpPipe(), PdpStages.PdpPipe())
        pl.add(PdpStages.PdpJoin(3))
        pl.add(PdpStages.PdpProcessor(count))
        pl.run([(0, 'string')])

    '''
    Forks and pipes should be allowed to mix
    '''

    def test_fork_pipe_mix(self):
        self.counts = Counter()

        def count(arg):
            _, string = arg
            self.counts[string] += 1
            if len(self.counts) == 2:
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

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpReplicatingFork(2))
        pl.add(PdpStages.PdpPipe(), PdpStages.PdpBalancingFork(2))
        pl.add(PdpStages.PdpProcessor(dont_manipulate),
               PdpStages.PdpProcessor(manipulate),
               PdpStages.PdpProcessor(manipulate))
        pl.add(PdpStages.PdpJoin(3))
        pl.add(PdpStages.PdpProcessor(count))
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

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpReplicatingFork(3))
        pl.add(PdpStages.PdpProcessor(dont_manipulate),
               PdpStages.PdpProcessor(dont_manipulate),
               PdpStages.PdpProcessor(manipulate))
        pl.add(PdpStages.PdpPipe(), PdpStages.PdpJoin(2))
        pl.add(PdpStages.PdpProcessor(dont_manipulate),
               PdpStages.PdpProcessor(manipulate))
        pl.add(PdpStages.PdpJoin(2))
        pl.add(PdpStages.PdpProcessor(count))
        pl.run([(0, 'string')])

    '''
    Processors and forks should not be allowed to mix
    '''

    def test_processor_fork_mix(self):
        def job(arg):
            return 'job'

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpReplicatingFork(2))
        with self.assertRaises(Exception) as e:
            pl.add(PdpStages.PdpProcessor(job), PdpStages.PdpBalancingFork(2))
        self.assertEqual(
            str(e.exception), 'Invalid types! All non PdpPipe objects in stage must be in same subclass')

    '''
    Processors and joins should not be allowed to mix
    '''

    def test_processor_join_mix(self):
        def job(arg):
            return 'job'

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpReplicatingFork(3))
        with self.assertRaises(Exception) as e:
            pl.add(PdpStages.PdpProcessor(job), PdpStages.PdpJoin(2))
        self.assertEqual(
            str(e.exception), 'Invalid types! All non PdpPipe objects in stage must be in same subclass')

    '''
    Forks and joins should not be allowed to mix
    '''

    def test_fork_join_mix(self):
        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpReplicatingFork(3))
        with self.assertRaises(Exception) as e:
            pl.add(PdpStages.PdpJoin(2), PdpStages.PdpBalancingFork(2))
        self.assertEqual(
            str(e.exception), 'Invalid types! All non PdpPipe objects in stage must be in same subclass')
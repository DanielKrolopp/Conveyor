from unittest import TestCase

from Pdp import PdpPipeline
from Pdp import PdpStages

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

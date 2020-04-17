from unittest import TestCase

from Pdp import PdpPipeline
from Pdp import PdpSteps

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
            pl.add(PdpSteps.PdpProcessor(finalize))
        except Exception:
            self.fail('Should not raise an exception: ' + str(e))

    '''
    Allow starting with valid pipeline stages
    '''

    def test_pipeline_valid_start(self):
        def finalize(arg):
            return True

        pl = PdpPipeline.PdpPipeline()

        # Pipe (should be allowed)
        try:
            pl.add(PdpSteps.PdpBalancingFork(2))
        except Exception as e:
            self.fail('Should not raise an exception: ' + str(e))

        # Balancing fork (should be allowed)
        try:
            pl.add(PdpSteps.PdpBalancingFork(2))
        except Exception as e:
            self.fail('Should not raise an exception: ' + str(e))

        # Replicating fork (should be allowed)
        try:
            pl.add(PdpSteps.PdpReplicatingFork(2))
        except Exception as e:
            self.fail('Should not raise an exception: ' + str(e))

        # Processor (should be allowed)
        try:
            pl.add(PdpSteps.PdpProcessor(finalize))
        except Exception as e:
            self.fail('Should not raise an exception: ' + str(e))

        # Join (should not be allowed at pipeline head)
        with self.assertRaises(Exception) as e:
            pl.add(PdpSteps.PdpMerge(2))
        self.assertEqual(
            str(e.exception), 'A PdpMerge must be preceeded by a PdpProcessor!')

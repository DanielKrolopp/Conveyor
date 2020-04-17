from unittest import TestCase

from Pdp import PdpPipeline
from Pdp import PdpSteps

'''
Test processors
'''


class TestProcessors(TestCase):
    '''
    Pipeline processors should only have valid callbacks
    '''

    def test_pipeline_valid_job(self):
        def finalize(arg):
            self.assertEqual(arg, 3)

        pl = PdpPipeline.PdpPipeline()

        # PdpProcessor should not take a non-function as an arg
        with self.assertRaises(Exception) as e:
            pl.add(PdpSteps.PdpProcessor("cookie"))
        self.assertEqual(
            str(e.exception), 'Invalid type! Pipeline processors must have a valid job!')

        # PdpProcessor should allow a function as an arg
        try:
            pl.add(PdpSteps.PdpProcessor(finalize))
        except Exception:
            self.fail('Should not raise an exception')

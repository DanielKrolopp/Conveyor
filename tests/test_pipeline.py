from unittest import TestCase

from Pdp import PdpPipeline
from Pdp import PdpSteps


class TestPipeline(TestCase):

    '''
    The most basic test. Create a pipeline, push in one value and retrieve it.
    '''

    def test_pipeline_basic(self):
        def finalize(arg):
            self.assertEqual(arg, 3)

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpSteps.PdpProcessor(finalize))
        pl.run([3])

    '''
    Processor -> Pipe -> Processor
    '''

    def test_pipeline_single_processor(self):
        def job(arg):
            return arg + 1

        def finalize(arg):
            self.assertEqual(arg, 4)

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpSteps.PdpProcessor(job))
        pl.add(PdpSteps.PdpPipe())
        pl.add(PdpSteps.PdpProcessor(finalize))
        pl.run([3])

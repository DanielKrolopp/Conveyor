from unittest import TestCase

from Pdp import PdpPipeline
from Pdp import PdpStages
from . import dummy_return_arg

'''
Test pipes
'''


class TestPipes(TestCase):
    '''
    Test that pipes can connect to pipes (including dummy head pipe)
    '''

    def test_pipes_connect_to_pipes(self):
        pl = PdpPipeline.PdpPipeline()

        # Pipes can connect to pipes
        try:
            pl.add(PdpStages.PdpPipe())
        except Exception:
            self.fail('Should not raise an exception')

        # Add another one
        try:
            pl.add(PdpStages.PdpPipe())
        except Exception:
            self.fail('Should not raise an exception')

    '''
    Implicit pipes should be allowed. Automatically insert a pipe between processors
    '''

    def test_implicit_pipes(self):
        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpProcessor(dummy_return_arg))

        # Add a second processor
        try:
            pl.add(PdpStages.PdpProcessor(dummy_return_arg))
        except Exception:
            self.fail('Should not raise an exception')

        # Add another one
        try:
            pl.add(PdpStages.PdpProcessor(dummy_return_arg))
        except Exception:
            self.fail('Should not raise an exception')

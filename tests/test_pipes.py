from unittest import TestCase

from conveyor.pipeline import Pipeline
from conveyor.stages import Pipe, Processor
from . import dummy_return_arg

'''
Test pipes
'''


class TestPipes(TestCase):
    '''
    Test that pipes can connect to pipes (including dummy head pipe)
    '''

    def test_pipes_connect_to_pipes(self):
        pl = Pipeline()

        # Pipes can connect to pipes
        try:
            pl.add(Pipe())
        except Exception:
            self.fail('Should not raise an exception')

        # Add another one
        try:
            pl.add(Pipe())
        except Exception:
            self.fail('Should not raise an exception')

    '''
    Implicit pipes should be allowed. Automatically insert a pipe between processors
    '''

    def test_implicit_pipes(self):
        pl = Pipeline()
        pl.add(Processor(dummy_return_arg))

        # Add a second processor
        try:
            pl.add(Processor(dummy_return_arg))
        except Exception:
            self.fail('Should not raise an exception')

        # Add another one
        try:
            pl.add(Processor(dummy_return_arg))
        except Exception:
            self.fail('Should not raise an exception')

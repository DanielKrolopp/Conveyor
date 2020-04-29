from unittest import TestCase

from conveyor.pipeline import Pipeline
from conveyor.stages import Pipe, Processor

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

        pl = Pipeline()

        # Processor should not take a non-function as an arg
        with self.assertRaises(Exception) as e:
            pl.add(Processor("cookie"))
        self.assertEqual(
            str(e.exception), 'Invalid type! Pipeline processors must have a valid job!')

        # Processor should allow a function as an arg
        try:
            pl.add(Processor(finalize))
        except Exception:
            self.fail('Should not raise an exception')

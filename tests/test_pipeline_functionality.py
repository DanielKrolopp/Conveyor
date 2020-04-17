from unittest import TestCase
from collections import Counter

from Pdp import PdpPipeline
from Pdp import PdpSteps
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

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpSteps.PdpProcessor(finalize))
        pl.run([3])

    '''
    Make sure that Processor -> Pipe -> Processor works.
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

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpSteps.PdpBalancingFork(2))
        pl.add(PdpSteps.PdpProcessor(job1, job2))
        pl.add(PdpSteps.PdpMerge(2))
        pl.add(PdpSteps.PdpProcessor(finalize))
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

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpSteps.PdpBalancingFork(2))
        pl.add(PdpSteps.PdpProcessor(job1, job2))
        pl.add(PdpSteps.PdpMerge(2))
        pl.add(PdpSteps.PdpProcessor(finalize))
        pl.run([False, False])

        self.assertEqual(self.counts['job1'], self.counts['job2'])

    '''
    Allow a pipeline to run multiple times without error.
    Note: currently, this hangs on the second run.
    '''

    def allow_multiple_pipeline_runs(self):
        def finalize(arg):
            self.assertEqual(self.counts['string'], self.counts['job2'])

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpSteps.PdpProcessor(dummy_return_arg))
        pl.add(PdpSteps.PdpPipe())
        pl.add(PdpSteps.PdpProcessor(dummy_return_arg))
        pl.add(PdpSteps.PdpPipe())
        pl.add(PdpSteps.PdpProcessor(finalize))
        pl.run(['string'])
        pl.run(['string'])

    '''
    Test forks and joins. 4 messages should come through. 3 of them are
    manipulated along the way.
    '''

    def fork_and_join1(self):
        self.counts = Counter()

        def finalize(arg):
            self.assertEqual(self.counts['string'], 1)
            self.assertEqual(self.counts['turing'], 2)
            self.assertEqual(self.counts['uvring'], 1)

        def job(arg):
            stage, string = arg
            l = list(string)
            l[stage] = chr(ord(l[stage]) + 1)
            return (stage + 1, ''.join(l))

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpSteps.PdpProcessor(job))
        pl.add(PdpSteps.PdpPipe())
        pl.add(PdpSteps.PdpBalancingFork(2))
        pl.add(PdpSteps.PdpPipe())
        pl.add(PdpSteps.PdpProcessor(job, dummy_return_arg))
        pl.add(PdpSteps.PdpReplicatingFork(2))
        pl.add(PdpSteps.PdpProcessor(
            job, dummy_return_arg, job, dummy_return_arg))
        pl.add(PdpSteps.PdpMerge(4))
        pl.add(PdpSteps.PdpProcessor(finalize))
        pl.run(['string', 'string'])

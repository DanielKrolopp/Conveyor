from unittest import TestCase
from collections import Counter
import sys
from threading import Lock
from time import sleep

from Pdp import PdpPipeline
from Pdp import PdpStages
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
        pl.add(PdpStages.PdpProcessor(finalize))
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
        pl.add(PdpStages.PdpProcessor(job))
        pl.add(PdpStages.PdpPipe())
        pl.add(PdpStages.PdpProcessor(finalize))
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
        pl.add(PdpStages.PdpBalancingFork(2))
        pl.add(PdpStages.PdpProcessor(job1), PdpStages.PdpProcessor(job2))
        pl.add(PdpStages.PdpJoin(2))
        pl.add(PdpStages.PdpProcessor(finalize))
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
        pl.add(PdpStages.PdpBalancingFork(2))
        pl.add(PdpStages.PdpProcessor(job1), PdpStages.PdpProcessor(job2))
        pl.add(PdpStages.PdpJoin(2))
        pl.add(PdpStages.PdpProcessor(finalize))
        pl.run([False, False])

        self.assertEqual(self.counts['job1'], self.counts['job2'])

    '''
    Allow a pipeline to run multiple times without error.
    Note: currently, this hangs on the second run.
    '''

    def test_allow_multiple_pipeline_runs(self):

        def finalize(arg):
            if arg == 'second':
                lock.release()

        lock = Lock()
        pl = PdpPipeline.PdpPipeline()

        pl.add(PdpStages.PdpProcessor(dummy_return_arg))
        pl.add(PdpStages.PdpPipe())
        pl.add(PdpStages.PdpProcessor(dummy_return_arg))
        pl.add(PdpStages.PdpPipe())
        pl.add(PdpStages.PdpProcessor(finalize))
        lock.acquire()
        pl.run(['first'])
        pl.run(['second'])
        sleep(1)
        self.assert_(lock.acquire(blocking=False))

    '''
    Test forks and joins. 4 messages should come through. 3 of them are
    manipulated along the way.
    '''

    def test_fork_and_join1(self):
        self.counts = Counter()

        def finalize(arg):
            return arg

        def job(arg):
            stage, string = arg
            l = list(string)
            l[stage] = chr(ord(l[stage]) + 1)
            return (stage + 1, ''.join(l))

        pl = PdpPipeline.PdpPipeline()
        pl.add(PdpStages.PdpProcessor(job))
        pl.add(PdpStages.PdpPipe())
        pl.add(PdpStages.PdpBalancingFork(2))
        pl.add(PdpStages.PdpPipe())
        pl.add(PdpStages.PdpProcessor(job),
               PdpStages.PdpProcessor(dummy_return_arg))
        pl.add(PdpStages.PdpReplicatingFork(2))
        pl.add(PdpStages.PdpProcessor(job), PdpStages.PdpProcessor(dummy_return_arg),
             PdpStages.PdpProcessor(job), PdpStages.PdpProcessor(dummy_return_arg))
        pl.add(PdpStages.PdpJoin(4))
        pl.add(PdpStages.PdpProcessor(finalize))
        pl.run(['string', 'string'])

        sys.stderr.write('Hey!', self.counts)
        self.assertEqual(self.counts['string'], 1)
        self.assertEqual(self.counts['turing'], 2)
        self.assertEqual(self.counts['uvring'], 1)

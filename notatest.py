from Pdp import PdpPipeline
from Pdp import PdpStages
import sys
from threading import Lock
from time import sleep

def dummy_return_arg(arg):
    return arg

def test_allow_multiple_pipeline_runs():

    def finalize(arg):
        if arg == 'second':
            lock.release()
            var = "B"
        print("Hi" + arg)

    var = "A"
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
    print(lock.acquire(blocking=False))
    print(var)

test_allow_multiple_pipeline_runs()
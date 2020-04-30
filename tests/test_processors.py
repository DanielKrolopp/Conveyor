from unittest import TestCase

from conveyor.pipeline import Pipeline
from conveyor.stages import Processor
from conveyor import common_memory

from multiprocessing import shared_memory
from time import sleep

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



    '''
    Processors should be able to share memory in a rudimentry way
    '''
    def test_processor_shared_memory(self):
   
        def worker1_task(args):
          shmem = shared_memory.SharedMemory(name=common_memory)
          buffer = shmem.buf
          buffer[:4] = bytearray([00, 11, 22, 33])
          shmem.close()

          return args

        def worker2_task(args):
          shmem = shared_memory.SharedMemory(name=common_memory)
          buffer = shmem.buf
          buffer[0] = 44
          shmem.close()

          return args

        def cleanup_task(args):
          shmem = shared_memory.SharedMemory(name=common_memory)
          import array
          print(array.array('b', shmem.buf[:4]))
          assert shmem.buf[0] == 44
          assert shmem.buf[1] == 11
          assert shmem.buf[2] == 22
          assert shmem.buf[3] == 33
            
          shmem.close()
          shmem.unlink()

          return args

        pl = Pipeline(shared_memory_amt=10)
        pl.add(Processor(worker1_task))
        pl.add(Processor(worker2_task))
        pl.add(Processor(cleanup_task))
        pl.run(['abc'])

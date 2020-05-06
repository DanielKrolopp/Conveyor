# Shared memory
__Note: the `common_memory` feature should be considered "very alpha" in that its specification and functionality are likely to change without warning. Additionally, `common_memory` requires Python 3.8.__

## Overview
In very specific instances, it's important to share memory across pipeline jobs and the main process without having to pass it through the pipeline. This is especially useful for when one needs to access variables or data "out of band" in that they are not part of the data stream on which the pipeline operates. Some use cases for this might be:

### Collecting metrics
For measuring processor metrics, it would make sense to maintain a global database, rather than returning a metrics object describing the operation of the processor. It's best to keep control and data variables separate for both programmer's sanity and for the sake of saving inter-processor bus bandwidth.

### Maintaining inter-processor state
In certain situations, multiple processors must be made stateful and must refer to a state variable stored elsewhere such that all processors can read it.

### Large, static blocks of data (ie: a pandas DataFrame)
This is especially true for training machine learning datasets. While it is possible to send a large DataFrame object to multiple processors via a replicating fork, a better idea would be to use a global read-only store. This would prevent duplication of large quantities of redundant data.

## Common pitfalls
It can be tempting to declare a global variable in your main application and then access it from within job function definitions. While this won't throw an error, it is important to remember that while Conveyor processors are written as functions, they are ultimately run as processes with their own view of a virtual memory space. This means that each processor, while all sharing a global variable name, will have its own independent copy of that variable.

Additionally, when modifying memory shared between processors, race conditions can occur. If concurrent reads/writes occur on shared memory, be sure to surround variable accesses with appropriate locks. Needless to say, these locks should also live in shared memory.

## Conveyor's `common_memory` functionality

When creating a pipeline, one can pass an optional argument of `shared_memory_amt`. This is the number of bytes in memory to allocate for global access. You can access this data from within a processor with `shared_memory.SharedMemory(name=common_memory)`.

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor
from conveyor import common_memory

from multiprocessing import shared_memory

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
```

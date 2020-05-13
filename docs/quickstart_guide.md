# Quickstart guide

The entire idea behind Conveyor is to process data as if it is moving through a stream (in Conveyor, this is called a pipeline). Each pipeline contains a series of stages in which data is manipulated in some way before being passed out of the other end of the pipeline. Each stage runs as its own process, meaning that when processing large batches of data, each transformation of the data can happen in parallel.

## Installation
Install Conveyor with

```python
pip install parallel-conveyor
```

## Pipelines
Pipelines form the core of Conveyor. They are the object through which data flows and is transformed. Initialize a pipeline with the following:

```python
from conveyor.pipeline import Pipeline

pl = Pipeline()
```

We can add stages to a pipeline using the `.add()` function, and run them using `.run()`. Additionally, we can use the `with` keyword to define a Pipeline to automatically free up resources used after we're done using the pipeline. You will often see this method used in our documentation.

```python
from conveyor.pipeline import Pipeline

with Pipeline() as pl:
    # Code involving the pipeline goes here
```
## Stages
We now need to add stages to our pipeline. Stages will allow us to process our data and acheive throughput to enhance the performance of our program. Stages are added using the Pipeline object's `.add()` function. Stages come in 4 main types:

### Processors
__Processors__ are the Conveyor's logical compute cores. Each processor will accept some data as input, transform that input and optionally return an output. Processors can maintain an internal state. Each of these processors is user-defined and wrapped in its own Python instance, allowing parallel execution of multiple processors at the same time. A user can specify sets of processors that should execute serially as joined by pipes or sets of processors that should act in parallel as defined by forks.

We can create a Processor with `Processor(job)` where the argument `job` serves as a callback to function that will run in parallel. Each callback should be written such that it has a single argument that represents a single block of data to be processed. This argument can be an integer, string, tuple, object or any other data type. At the end of the function, we must return another single block of data to be handled by the next stage in the pipeline.

Writing jobs as functions is relatively straightforward: when the pipeline runs, each stage will receive a data block as a function argument, perform its processing, then spit out a result as a return value. Additionally, single-argument functions from other libraries or Python's standard library can be used (ie: `print()`).

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor

def job(arg):
    return arg + 1

with Pipeline() as pl:
    pl.add(Processor(job))
```

### Pipes
__Pipes__ serve as links between processors. They act as a stream, transporting data from one point in the pipeline to another. Pipes are implemented as a producer-consumer buffer where a leading processor acts as the producer and a trailing processor acts as a consumer. They don't use any processing power on their own and can be implicitly added by Conveyor to fill in gaps between processors.

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor, Pipe

def job(arg):
    return arg + 1

with Pipeline() as pl:
    pl.add(Processor(job))
    pl.add(Pipe())
    pl.add(Processor(job))
```

Equivalently, we could write the same code without the Pipe, as Conveyor will add it implicitly.

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor, Pipe

def job(arg):
    return arg + 1

with Pipeline() as pl:
    pl.add(Processor(job))
    pl.add(Processor(job))
```

### Forks
__Forks__ serve as a way of splitting data travelling through the pipeline in two different ways. Forks can act in a _replicating_ fashion, where they duplicate all data coming in and push it to many output processors or in a _balancing_ fashion, where they perform a load-balancing operation, dividing input data blocks over a number of output processors.

Both types of forks take in a single argument: the number of output pipes to which they will attach. The number of stages added in parallel with the next call to `.add()` must match the number of output pipes at the previous stage. Processors, forks and pipes can all exist in parallel at the same stage in a pipeline.

#### Replicating Forks
__Replicating Forks__ allow one processor to split output data into multiple copies so that multiple processors can then perform operations using the entire output data. For example, this would allow multiple different ML models to be trained and tested in parallel. The input-output numbering of the many-to-one relationship of forks is primarily user defined.

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor, Pipe, ReplicatingFork, Join

def job(arg):
    return arg + 1

with Pipeline() as pl:
    pl.add(ReplicatingFork(2))
    pl.add(Processor(job), Pipe())
```

#### Balancing Forks
__Balancing Forks__ allow one processor to balance a stream of data over multiple consumer processors. This will serve to minimize the effect of pipe stalling for larger data sets. The input-output numbering of the many-to-one relationship of forks is primarily determined by pipe stalling detected at runtime and the number of physical cores available.

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor, Pipe, BalancingFork, Join

def job(arg):
    return arg + 1

with Pipeline() as pl:
    pl.add(BalancingFork(2))
    pl.add(Processor(job), Pipe())
```

### Joins
__Joins__ allow multiple processors to combine their data streams into one logical pipe. The combined stream can then be forked again for the next step of the pipeline, processed by a single processor, or serve as output at the end of the pipeline.

This means that the outputs from the previous parallel stages can be interleaved arbitrarily. Joins are useful when output from multiple different processors need to be serialized and work in a first-come-first-serve manner.

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor, Pipe, ReplicatingFork, Join

def job(arg):
    return arg + 1

with Pipeline() as pl:
    pl.add(ReplicatingFork(2))
    pl.add(Processor(job), Pipe())
    pl.add(Join(2))
    pl.add(Processor(print))
```

## Running a pipeline
The final step is to run our pipeline that we just created.To do so, we call `Pipeline.run()` on an array of input data objects, where each item will be passed through the pipeline in the same order as in the array. At any given time, multiple items in the array will be in the pipeline, albeit at different stages. Running the following trivial example:

```python
from conveyor.pipeline import Pipeline
from conveyor.stages import Processor, Pipe, ReplicatingFork, Join

def job(arg):
    return arg + 1

with Pipeline() as pl:
    pl.add(ReplicatingFork(2))
    pl.add(Processor(job), Pipe())
    pl.add(Join(2))
    pl.add(Processor(print))
    pl.run([2, 9, 11])
```

Yields this output (separated by new lines): `3 10 12 2 9 11`.

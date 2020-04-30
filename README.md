# Conveyor - simple, intuitive pipelining
Conveyor is a multiprocessing framework to facilitate writing data pipelines and job systems in Python. The Conveyor provides five main features a developer can make use of: __processors, pipes, replicated forks, balanced forks__  and __joins__.

### Processors
__Processors__ are the Conveyor's logical compute cores. Each processor will take in some data as input, transform that input and optionally return an output. Processors can maintain an internal state. Each of these processors is user-defined and wrapped in its own Python instance, allowing parallel execution of multiple processors at the same time. A user can specify sets of processors that should execute serially as joined by pipes or sets of processors that should act in parallel as defined by forks.

### Pipes
__Pipes__ serve as links between processors. They act as a stream, transporting data from one point in the pipeline to another. Pipes are implemented as a producer-consumer buffer where a leading processor acts as the producer and a trailing processor acts as a consumer.

### Replicated Forks
__Replicated Forks__ allow one processor to split output data into multiple copies so that multiple processors can then perform operations using the entire output data. This will allow multiple different ML models to be trained and tested in parallel. The input-output numbering of the many-to-one relationship of forks is primarily user defined.

### Balanced Forks
__Balanced Forks__ allow one processor to balance a stream of data over multiple consumer processors. This will serve to minimize the effect of pipe stalling for larger data sets. The input-output numbering of the many-to-one relationship of forks is primarily determined by pipe stalling detected at runtime and the number of physical cores available.

### Joins
__Joins__ allow multiple processors to combine their data streams into one logical pipe. The combined stream can then be forked again for the next step of the pipeline, processed by a single processor, or serve as output at the end of the pipeline.

# Rules for constructing pipelines
In order to allow developers to use the pipeline however they see fit, Conveyor attempts to be unopinionated, however there must be some rules to keep the pipelines from becoming nonsensical or a programming language of it's own. The pipeline should be kept simple and most of the program's logic should be done in the processor steps.
- All elements in the pipeline must be Conveyor Types
- A Pipeline must start with a process
- If not starting the pipeline, every Processor must be preceded by a Pipe, Fork, or Join
    - If a Pipe is not explicitly created, it will be automatically inserted, however a warning will be raised
- Every Fork and Join must be preceded by a pipe

# Testing
To run the tests, ensure nose is installed and run nosetests from the project directory

`pip3 install nose && nosetests`

# Building from source
To build the distribution archives, you will need the latest version of setuptools and wheel.

`python3 -m pip install --user --upgrade setuptools wheel`

Run `setup.py` to build using the following command:

`python3 setup.py sdist bdist_wheel`

The compiled `.whl` and `.tar.gz` files will be in the `/dist` directory.

# Efficiency guide

If you're using Conveyor, you're using it because you want to squeeze every
possible drop of performance out of your application. This should serve as a
short guide of performance-breaking scenarios and how to avoid them.

## Run pipelines using `with` statements
Pipelines can be run in two different ways, as demonstrated with the code
snippets below:

```python3
pl = Pipeline()
pl.add(job)

pl.run([data, data2, data3])
pl.run([data4, data5, data6])
```

or...

```python3
with Pipeline() as pl:
    pl.add(job)

    pl.run([data, data2, data3])
    pl.run([data4, data5, data6])
```

When running a pipeline multiple times, Conveyor encourages users to use the
second option described above. In the first case, heavyweight processes are
created and killed (called 'opening' and 'closing' the pipeline, respectively)
at the start and end of each invocation of `.run()`. This is disadvantageous,
because creating and killing processes takes a large length of time. It would
be much better to create the processes in the pipeline, use them on the first
invocation of `.run()`, keep them running, and then use them again on the
second invocation of `.run()`. This is what the second case does, where the
pipeline is implicitly opened and closed at the start and end of the `with`
statement.

## Use shared memory for read-only data
Passing large quantities of data between processors is expensive. It's
advantageous to use a read-only memory cache whenever feasible. See the
[shared memory guide](shared_memory.md) for more details.

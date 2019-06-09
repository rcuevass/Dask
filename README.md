# Dask
A repository with simple scripts to learn Dask and compare its
performance versus pandas, H2O and Spark.


## Dask

Code in `main.py` has been tested by the use of scheduler and workers
accorging to the following executions:

* `\apps\Anaconda3\pkgs\distributed-1.25.1-py37_0\Scripts>python dask-scheduler-script.py`
* `\apps\Anaconda3\pkgs\distributed-1.25.1-py37_0\Scripts>python dask-worker-script.py ip_provided_by_scheduler:8786 --nprocs 6 --nthreads 1`
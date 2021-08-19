# Time Series Data Analytical Toolkit (TSDAT)

The Time Series Data Analytical Toolkit (TSDAT) is an open-source python framework
for creating pipelines to read-in, standardize, and enhance time series datasets of
any dimensionality for use in scalable applications and data repositories. 


## Important Links

* Tsdat Documentation: https://tsdat.readthedocs.io
* Xarray Documentation: https://xarray.pydata.org
* Template Repositories: https://github.com/tsdat/template-repositories
* Issues: https://github.com/tsdat/tsdat/issues


## Getting Started

`tsdat` uses [xarray](https://github.com/pydata/xarray) in some capacity for nearly 
all components of its data pipelines, so we highly recommend checking out their 
[documentation](https://xarray.pydata.org) if you have not used `xarray` before
or if you need a refresher.

We recommend starting by [reading the docs](https://tsdat.readthedocs.io) to get a
high-level overview of `tsdat` and the following components:
* `FileHandler` and `Storage` classes for abstracting I/O
* `Pipeline` and `IngestPipeline` base classes for creating `tsdat` data pipelines
* `QualityChecker` and `QualityHandler` classes for testing and managing data quality
* `Pipeline Config` and `Storage Config` yaml configuration files

After you have a basic understanding of the various tsdat components, we recommend 
using the [local ingest template](https://github.com/tsdat/ingest-template-local) to 
create a data ingestion pipeline that runs on your computer. Follow the instructions 
outlined there to install the dependencies and run the included example.

*Alternatively*, if you want to start from scratch and use `tsdat` without starting
from a template, you can install `tsdat` with `pip` like so:

```
pip install tsdat
```

and then use `tsdat` however you like in your project.

## Contributing

We enthusiastically welcome contributions to any of our repositories. 

If you find a bug or want to submit a feature request, please 
[submit an issue](https://github.com/tsdat/tsdat/issues). If you are submitting an
issue for a bug, please explain the bug and provide a minimal example that reproduces
the bug. Feature requests should clearly explain what the new feature is and what the
benefit of this feature would be.  

If you know how to fix a bug or implement a feature request and would like to contribute
code to help resolve an open issue, please submit a 
[pull request](https://github.com/tsdat/tsdat/pulls). See below for guidelines on how to 
get started with a pull request:

1. Fork `tsdat` to `<your_username>/tsdat` and clone it to your working area.
2. Install `tsdat` from source by running `pip install -e .` from the cloned repository.
3. Verify that you can still reproduce the bug or that the feature has not been implemented yet.
4. Make your changes. Be sure to test and to update the `docs/` folder if appropriate.
6. Ensure your changes on your remote fork of `tsdat` and submit a [PR](https://github.com/tsdat/tsdat/pulls).

# Time Series Data Analytical Toolkit (TSDAT)


[![PyPI version](https://badge.fury.io/py/tsdat.svg)](https://badge.fury.io/py/tsdat)
[![DOI](https://zenodo.org/badge/306085871.svg)](https://zenodo.org/badge/latestdoi/306085871)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[![main](https://github.com/tsdat/tsdat/actions/workflows/pytest.yml/badge.svg)](https://github.com/tsdat/tsdat/actions/workflows/pytest.yml)
[![Documentation Status](https://readthedocs.org/projects/tsdat/badge/?version=latest)](https://tsdat.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/tsdat/tsdat/branch/main/graph/badge.svg?token=W2FHMSQLEH)](https://codecov.io/gh/tsdat/tsdat)
[![Maintainability](https://api.codeclimate.com/v1/badges/e82e8c5103f4eb3a5686/maintainability)](https://codeclimate.com/github/tsdat/tsdat/maintainability)



The Time Series Data Analytical Toolkit (TSDAT) is an open-source python framework
for creating pipelines to read, standardize, and enhance time series datasets of
any dimensionality for use in scalable applications and data repositories. 


## Important Links

* Tsdat Documentation: https://tsdat.readthedocs.io
* Template Repositories: https://github.com/tsdat/template-repositories
* Issues: https://github.com/tsdat/tsdat/issues
* Xarray Documentation: https://xarray.pydata.org


## Getting Started


We recommend starting by [reading the docs](https://tsdat.readthedocs.io) to get a
high-level overview of `tsdat`. 

After you have a basic understanding of the various tsdat components, we recommend 
using the [local ingest template](https://github.com/tsdat/ingest-template-local) to 
create a data ingestion pipeline that runs on your computer. Follow the instructions 
outlined there to install the dependencies and run the included example.



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
2. Install development requirements: `pip install -r requirements-dev.txt`
3. Make your changes and update `tests` or `docs` as appropriate. 
4. Test your changes by running `pytest`, `black --check .`, and `flake8`
5. Submit a [Pull Request](https://github.com/tsdat/tsdat/pulls) and provide a detailed 
description of your work so we can review your changes as efficiently as possible.

# Time Series Data Library
This library provides general utility methods for working with 
time series datasets, which are stored as Xarray Dataset objects.
In particular, it will provide declarative methods for being able
standardize, apply Q/C checks, correct, and transform datastreams
as a whole, reducing the amount of coding required for data
processing.
test
# Getting Started

## Installation
You can install tsdat and its dependencies using pip

```bash
pip install tsdat
```

## Documentation
For help using tsdat, please see our documentation at
https://tsdat.readthedocs.io/

# Docker
Please see https://hub.docker.com/orgs/tsdat for the list of available 
tsdat docker images.

# Installation from Source
If you will be developing/contributing to the tsdat code base,
first clone the repository from 

```bash
git clone https://github.com/tsdat/tsdat.git
```

You can install the tsdat  requirements via:

```bash
pip install -r requirements.txt
```

## Releasing to pypi
TODO: to be replaced by CICD build instead of manual process.

### Prereq: Make sure that you have twine installed
```bash
pip install twine
```

### 1) Update the version numbers
1. setup.py
2. docker/docker-compose.yml
3. docker/build.sh

Then commit tsdat with the new build numbers.

### 2) Then deploy the new release.

```bash
cd tsdat
python setup.py sdist bdist_wheel
twine upload dist/*
```

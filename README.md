<p align="center">
   <img src="./docs/figures/tsdat_logo.svg" width="700" style="max-width: 700px;">
</p>

<p align="center">
<a href=../../actions/workflows/pytest.yml>
    <img src="https://github.com/tsdat/tsdat/actions/workflows/pytest.yml/badge.svg">
</a>
<a href=https://tsdat.readthedocs.io/en/latest/?badge=latest>
    <img src="https://readthedocs.org/projects/tsdat/badge/?version=latest">
</a>
<a href=https://badge.fury.io/py/tsdat>
    <img src="https://badge.fury.io/py/tsdat.svg">
</a>
<a href=https://pepy.tech/project/tsdat>
    <img src="https://pepy.tech/badge/tsdat">
</a>
<a href="https://zenodo.org/badge/latestdoi/306085871">
    <img src="https://zenodo.org/badge/306085871.svg">
</a>
<!-- <a href="https://hub.docker.com/r/tsdat/tsdat-lambda">
    <img src="https://img.shields.io/docker/pulls/tsdat/tsdat-lambda.svg?color=%2327B1FF&logoColor=%234D606E">
</a> -->
</p>
<p align="center">
<a href=https://github.com/psf/black>
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg">
</a>
<a href="https://codecov.io/gh/tsdat/tsdat">
    <img src="https://codecov.io/gh/tsdat/tsdat/branch/main/graph/badge.svg">
</a>
<a href="https://codeclimate.com/github/tsdat/tsdat/maintainability">
    <img src="https://api.codeclimate.com/v1/badges/e82e8c5103f4eb3a5686/maintainability">
</a>
</p>

# About Tsdat

Tsdat is an open-source python framework for declaratively creating pipelines to read,
standardize, and enhance time series datasets of any dimensionality for use in scalable
applications and in building large data repositories.

This repository contains the core tsdat code. We invite you to explore this, especially
for those willing to provide feedback or make contributions to the tsdat core (we
enthusiastically welcome issues, PRs, discussions & new ideas, etc.).

> Most users should start with a [template repository](https://github.com/tsdat/template-repositories)
to generate boilerplate code and configurations needed to create a tsdat data pipeline.
We recommend **[this template](https://github.com/tsdat/pipeline-template)** to start
with, as it is the most flexible and well-supported template that we offer.

# Development Environment

Instructions on setting up your development environment for working on the core tsdat
code are included below:

1. Fork this repository to your github account and open it on your desktop in an IDE of
your choice.

    > We recommend using VS Code, as we've included extra settings that make it easy to
    start developing in a standard environment with no overhead configuration time.

2. Open an appropriate terminal shell from your computer
   1. If you are on Linux or Mac, just open a regular terminal
   2. If you are on Windows, start your Anaconda prompt if you installed Anaconda
   directly to Windows, OR open a WSL terminal if you installed Anaconda via WSL.

3. Run the following commands to create and activate your `conda` environment

    ```shell
    conda env create
    conda activate tsdat
    pip install -e ".[dev]"
    ```

# Community

Tsdat is an open-source repository and we highly-value community contributions and
engagement via [issues](https://github.com/tsdat/tsdat/issues),
[pull requests](https://github.com/tsdat/tsdat/pulls), and
[discussions](https://github.com/tsdat/tsdat/discussions). Please let us know if you
find bugs, want to request new features, or have specific questions about the framework!

# Additional resources

- Learn more about `tsdat`:
  - GitHub: <https://github.com/tsdat>
  - Documentation: <https://tsdat.readthedocs.io>
  - Data standards: <https://github.com/tsdat/data_standards>
  - Preferred template: <https://github.com/tsdat/pipeline-template>
  - All templates: <https://github.com/tsdat/template-repositories>
  - Docker Images: <https://hub.docker.com/u/tsdat>
- Learn more about `xarray`:
  - GitHub: <https://github.com/pydata/xarray>
  - Documentation: <https://xarray.pydata.org>
- Learn more about `act-atmos`:
  - GitHub: <https://github.com/arm-doe/act>
  - Documentation: <https://arm-doe.github.io/ACT/>
- Other useful tools:
  - VS Code: <https://code.visualstudio.com/docs>
  - Docker: <https://docs.docker.com/get-started/>
  - `pytest`: <https://github.com/pytest-dev/pytest>
  - `black`: <https://github.com/psf/black>
  - `matplotlib` guide: <https://realpython.com/python-matplotlib-guide/>

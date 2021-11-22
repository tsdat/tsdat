<p align="center">
   <img src="./docs/source/figures/tsdat_logo.svg" width="700" style="max-width: 700px;">
</p>

<p align="center">
<a href=https://github.com/tsdat/tsdat/actions/workflows/pytest.yml>
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


# About tsdat

tsdat is an open-source python framework for declaratively defining creating pipelines
to read, standardize, and enhance time series datasets of any dimensionality for use in
scalable applications and in building large data repositories.

This repository contains the core tsdat code. We invite you to explore this, especially
if you are willing to provide feedback or even help contribute to the tsdat core (we
enthusiastically welcome all contributions), however most users should start with a
[template repository](https://github.com/tsdat/template-repositories) to generate
nearly all of the code and configurations you need to create a declarative data
pipeline with tsdat. We recommend **[this template](
https://github.com/tsdat/ingest-template)** to start with, as it is the most flexible
and well-supported template that we offer.


# Development setup

We recommend that developers using tsdat or developers working on tsdat set up their
environment according to the following guidelines:


1. Download and install [VS Code](https://code.visualstudio.com). Make sure to add 
`code` to your path if prompted.

    We chose VS Code because of its clean user interface, quick startup time, extremely
    powerful capabilities out-of-box, and its rich library of open source extensions.

2. Clone your fork of this repository to your laptop and open it up in VS Code

3. The first time you open this project in VS Code you will be prompted to install the
recommended extensions. Please do so now.

4. **Windows users**: We recommend using
[Docker](https://www.docker.com/products/docker-desktop) to manage dependencies for
this project. If you choose to use Docker follow the steps below:
    - Press `F1` (or `ctrl-shift-p`) to bring up the command pane in VS Code
    - In the command pane, type: `Remote-Containers: Open Folder in Container...` and
    hit `return`
    - You will be prompted to specify which folder should be opened. Select the folder
    containing this `README` file
    - Several dialog boxes may appear while the VS Code window is refreshing. Please
    install the recommended extensions via the dialog box. An additional dialog box
    should appear asking you to reload the window so Pylance can take effect. Please do
    this as well.
    - After the window refreshes your development environment will be set up correctly.
    You may skip steps 5. and 6.

    You can find more information about VS Code and docker containers
    [here](https://code.visualstudio.com/docs/remote/containers).

5. We highly recommend using [conda](https://docs.anaconda.com/anaconda/install/) to
manage dependencies in your development environment. Please install this using the link
above if you haven't already done so. Then run the following commands to create your
environment:
    
    ```bash
    $ conda create --name tsdat python=3.8
    $ conda activate tsdat
    (tsdat) $ pip install -r requirements-dev.txt
    ```

6. Tell VS Code to use your new `conda` environment:
    - Press `F1` (or `ctrl-shift-p`) to bring up the command pane in VS Code
    - In the command pane, type: `Python: Select Interpreter` and hit `return`
    - Select the newly-created `tsdat` conda environment from the list. Note
    that you may need to refresh the list (cycle icon in the top right) for it to show
    up.
    - Reload the VS Code window to ensure that this setting propagates correctly.
    This is probably not needed, but doesn't hurt. To do this, press `F1` to open
    the control pane again and type `Developer: Reload Window`.


# Contributing

We enthusiastically welcome contributions to any of our repositories.

If you find a bug or want to submit a feature request, please [submit an issue](
https://github.com/tsdat/tsdat/issues). If you are submitting an issue for a bug,
please explain the bug and provide a minimal example that reproduces the bug. Feature
requests should clearly explain what the new feature is and what the benefit of the
feature would be.

If you know how to fix a bug or implement a feature request and would like to
contribute code to help resolve an open issue, please submit a [pull request](
https://github.com/tsdat/tsdat/pulls). See below for guidelines on how to get started
with a pull request:

1. Fork `tsdat` to `<your_username>/tsdat` and clone it to your working area.
2. Install development requirements: `pip install -r requirements-dev.txt`
3. Make your changes and update `tests` or `docs` as appropriate.
4. Test your changes by running `pytest`, `black --check .`, and `flake8`
5. Submit a [Pull Request](https://github.com/tsdat/tsdat/pulls) and provide a detailed
description of your work so we can review your changes as efficiently as possible.

# Additional resources

- Learn more about `tsdat`:
    - GitHub: https://github.com/tsdat
    - Documentation: https://tsdat.readthedocs.io
    - Data standards: https://github.com/tsdat/data_standards
    - Preferred template: https://github.com/tsdat/ingest-template
    - All templates: https://github.com/tsdat/template-repositories
    - Docker Images: https://hub.docker.com/u/tsdat
- Learn more about `xarray`: 
    - GitHub: https://github.com/pydata/xarray
    - Documentation: https://xarray.pydata.org
- Learn more about `act-atmos`: 
    - GitHub: https://github.com/arm-doe/act
    - Documentation: https://arm-doe.github.io/ACT/
- Other useful tools:
    - VS Code: https://code.visualstudio.com/docs
    - Docker: https://docs.docker.com/get-started/
    - `pytest`: https://github.com/pytest-dev/pytest
    - `black`: https://github.com/psf/black
    - `matplotlib` guide: https://realpython.com/python-matplotlib-guide/

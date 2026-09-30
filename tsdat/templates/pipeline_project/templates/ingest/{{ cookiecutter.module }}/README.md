# {{ cookiecutter.ingest_name }} Ingestion Pipeline

The {{ cookiecutter.ingest_name }} ingestion pipeline was created from a cookiecutter template. This README file contains
instructions for running and testing your pipeline.

## Prerequisites

* Ensure that your development environment has been set up according to
[the instructions](../../README.md#development-environment-setup).

> **Windows Users** - Make sure to run your `conda` commands from an Anaconda prompt OR from a WSL shell with miniconda
> installed. If using WSL, see [this tutorial on WSL](https://tsdat.readthedocs.io/en/latest/tutorials/wsl.html) for
> how to set up a WSL environment and attach VS Code to it.

* Make sure to activate the tsdat-pipelines anaconda environment before running any commands:  `conda activate tsdat-pipelines`

## Running your pipeline
This section shows you how to run the ingest pipeline created by the template.  Note that `{ingest-name}` refers
to the pipeline name you typed into the template prompt, and `{location}` refers to the location you typed into
the template prompt.

1. Make sure to be at your $REPOSITORY_ROOT. (i.e., where you cloned the pipeline-template repository)


2. Run the runner.py with your test data input file as shown below:

```bash
cd $REPOSITORY_ROOT
conda activate tsdat-pipelines # <-- you only need to do this the first time you start a terminal shell
python runner.py ingest pipelines/{ingest-name}/test/data/input/{location}_data.csv
```

## Test data
Out of the box, your pipeline comes with some initial test data located in the  `pipelines/{ingest-name}/test/data/input/`
folder.  This folder is meant to store data used for regression tests that will run before your
pipeline is deployed to ensure that it is functioning properly.  In addition to the `input` test data folder, there is also
an `expected` test data folder.  After you edit your pipeline definition and verify that the output is correct, you
should place your new input test data into the `input` folder and your validated output file into the `expected` folder.
If your input and expected output files have different names from the ones that came out of the box, you should update
the `test_pipeline.py` file to point to the new files.

## Testing your pipeline
This template is set up with a pytest unit test to ensure your pipeline is working correctly.  It is intended that the
pytest unit tests will be run automatically before pipeline deployment to prevent against breaking code changes.  To
run your tests locally, run these commands from your anaconda environment shell:

```bash
cd $REPOSITORY_ROOT
pytest
```

## Customizing your pipeline
You will need to edit the configuration files and possibly additional python code (e.g., pipeline.py) to customize
the template pipeline for your data.  To assist with customizing  your pipeline, this template comes embedded with
pre-configured VS Code settings that will make editing/running/debugging your pipeline much easier.  Therefore,
we highly recommend using the VS Code IDE to customize your pipeline.  However, advanced Python developers may also use any
other IDE of choice (e.g., PyCharm).

1. Use the `TODO-Tree` VS Code extension or use the search tool to find occurrences of
"`# DEVELOPER:`". Each instance of this requires your attention. Attend to all the
developer todos in this folder and remove the comment as you implement things. These
developer comments will need to be removed before the pipeline is deployed.


2. As you are developing, try to follow best practices to save yourself (and others)
time in the future:
    - Commit your changes to git/github early and often to prevent accidental code loss.
    - Write tests as soon as possible and test often. When pushing changes to github
    all the tests in this repository will be run.
    - In general, try to write modular, reuseable code to save your future self (and
    your team members) some time. Comments are particularly useful when they explain
    **why** something was done, as opposed to *how* it was done.


3. You can run your code locally in by running the tests or by running the `runner.py` script 
described in the sections above.  To debug your code in VS Code, you can use the `Debug Tests` launch
configuration that comes included with this template.  

    [Click here for more help with debugging in VS Code](https://code.visualstudio.com/docs/python/debugging?msclkid=0583222dc7dc11ecbd2da2b120e82795 'Learn VS Code')


4. When you have finished customizing your pipeline, at a minimum your tests should pass. 


5. This template has come pre-configured with VS Code code style checkers (e.g., `linters`).  So if you 
develop using VSCode, your code will be automatically formatted to use flake8 style conventions.  To disable
style checking for a specific line, add "`# noqa`" to the end of the line.


6. This template has come pre-configured with Python type hint type checking using mypy.  This will
result it red error lines showing up in your VS Code editor if you use type inconsistencies (e.g. passing a string into 
a function that is declared to take an array as a parameter).  If you would like to turn off the mypy linting,
you can edit the `.vscode/settings.json` file and disable it as follows:
    ```json
    "python.linting.mypyEnabled": false,
    ```

    [Click here for more information on configuring Python linters in VS Code.](https://code.visualstudio.com/docs/python/linting)

   
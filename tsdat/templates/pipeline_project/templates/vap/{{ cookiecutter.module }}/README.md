# {{ cookiecutter.vap_name }} Transformation Pipeline

The {{ cookiecutter.vap_name }} transformation pipeline was created from a cookiecutter template. This README file contains
instructions for running and testing your pipeline.

## Prerequisites

* Ensure that your development environment has been set up according to
[the instructions](../../README.md#development-environment-setup).

## Running your pipeline

1. Navigate to the repository root from the terminal (i.e., 2 levels up from this file)
2. Run `runner.py` and specify the transformation pipeline that should run:

        ```shell
        python runner.py vap pipelines/{{ cookiecutter.module }}/config/pipeline.yaml -b 20230324 -e 20230325
        ```


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

   
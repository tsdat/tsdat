# Tsdat Pipeline Project

This project was generated with `tsdat init` from the tsdat bundled pipeline template.
The original starter is [tsdat/pipeline-template](https://github.com/tsdat/pipeline-template).

This repository contains a collection of one or more `tsdat` pipelines 
(as found under the ``pipelines`` folder).  This enables related pipelines 
to be more easily maintained and run together.  New pipelines can be added 
easily via the template mechanism described below.

## Repository Structure

The repository is made up of the following core pieces:

- **`tsdat ingest` / `tsdat vap`**: Installed commands for running pipelines.

- **`pipelines/*`**: Collection of custom data pipelines using `tsdat`.

- **`pipelines/example_pipeline`**: An out-of-the-box example `tsdat` pipeline.

- **`templates/*`**: Template(s) used to generate new pipelines.

- **`shared/*`**: Shared configuration files that may be used across multiple pipelines.

- **`utils/*`**: Utility scripts.

## Prerequisites

The following are required to develop a `tsdat` pipeline:

1. **Python and tsdat.** Install `tsdat` to create additional pipelines.
   A GitHub account is only needed if you want to host this repository on GitHub.

2. **An Anaconda environment.**  We strongly recommend developing in an Anaconda Python 
environment to ensure that there are no library dependency issues.  
[Click here for more information on installing Anaconda on your computer](https://docs.anaconda.com/anaconda/install/index.html)

    > **Windows Users** - You can install Anaconda directly to your Windows 
    box OR you can run via a linux environment using the Windows Subsystem 
    for Linux (WSL).  See
    [this tutorial on WSL](https://tsdat.readthedocs.io/en/latest/tutorials/setup_wsl.html) 
    for how to set up a WSL environment and attach VS Code to it.

## Creating a new repository

Run `tsdat init my-pipelines` from a directory where you want the project to live.
The command creates `my-pipelines/` with this example and the pipeline templates.
Run `cd my-pipelines && git init` if you want to track it with Git.
Alternatively, you can create a repository using the upstream GitHub template:

1. Click this '[Use this template](https://github.com/tsdat/pipeline-template/generate)' 
link and follow the steps to copy the template repository into to your account.
    > **NOTE:** If you are looking to get an older version of the template, you 
    will need to select the box next to 'Include all branches' and set the branch 
    your are interested in as your new default branch.

2. On github click the 'Code' button to get a link to your code, then run

    ```shell
    git clone <the link you copied>
    ```

    from the terminal on your computer where you would like to work on the code.

## Setting up your Anaconda environment

1. Open a terminal shell from your computer
   - Linux or Mac: open a regular terminal
   - Windows: open an Anaconda prompt if you installed Anaconda directly
   to Windows, OR open a WSL terminal if you installed Anaconda via WSL.

2. Run the following commands to create and activate your conda environment:

    ```shell
    conda env create
    conda activate tsdat-pipelines
    ```

3. Verify your environment is set up correctly by running the tests for this repository:

    ```shell
    pytest
    ```

    If you get the following warning message when running the test:

    ```shell
    UserWarning: pyproj unable to set database path.
    ```

    Then run the following additional commands to permanently remove this warning message:

    ```shell
    conda remove --force pyproj
    pip install pyproj
    ```

    If everything is set up correctly then all the tests should pass.

## Opening your repository in VS Code

1. Open the cloned repository in VS Code. *(This repository contains default settings for
VS Code that will make it much easier to get started quickly.)*

2. Install the recommended extensions (there should be a pop-up in VS Code with recommendations).

    > **Windows Users**: In order to run python scripts in VSCode, follow steps A-C below:

    A. Install the extension Code Runner (authored by Jun Han).

    B. Press `F1`, type `Preferences: Open User Settings (JSON)` and select it.

    C. Add the following lines to the list of user settings, and update `<path to anaconda>` for
    your machine:

    ```json
    {
        "terminal.integrated.defaultProfile.windows": "Command Prompt",
        "python.condaPath": "C:/<path to anaconda>/Anaconda3/python.exe",
        "python.terminal.activateEnvironment": true,
        "code-runner.executorMap": {
            "python": "C:/<path to anaconda>/Anaconda3/Scripts/activate.bat && $pythonPath $fullFileName"
        },
    }
    ```

3. Tell VS Code to use your new conda environment:
    - Press `F1` to bring up the command pane in VS Code
    - Type `Python: Select Interpreter` and select it.
    - Select the newly-created `tsdat-pipelines` conda environment from the drop-down list. 
    You may need to refresh the list (cycle icon in the top right) to see it.
    - Bring up the command pane and type `Developer: Reload Window` to reload VS Code
    and ensure the settings changes propagate correctly.

4. Verify your VS Code environment is set up correctly by running the tests for this repository:
    - Press `F1` to bring up the command pane in VS Code
    - Type `Test: Run All Tests` and select it
    - A new window pane will show up on the left of VS Code showing test status
    - Verify that all tests have passed (Green check marks)

5. Set up yaml validation: run `tsdat generate-schema` from the command line

    > NOTE: if you would like to validate your configuration files using 
    one of the supported standards (i.e., Attribute Conventions for Data
    Discovery (ACDD) or the Ingrated Ocean Observing System (IOOS), then 
    please use the `--standards` flag and pass either `acdd` or `ioos`.

    > I.e. `tsdat generate-schema --standards ioos`

## Processing Data

From the repository root, use the installed `tsdat` command to run ingest and VAP
pipelines. Install the project's requirements and activate its environment first.

### Ingest Pipelines

- The lowest level pipelines that read in raw data are our ingest pipelines.
They are run via the following command:
    ```shell
    tsdat ingest <path(s) to file(s) to process>
    ```
    > The pipeline(s) used to process the data will depend on the specific patterns declared
    by the `pipeline.yaml` files in each pipeline module in this repository.

- You can run the example pipeline that comes bundled with this repository by running:

    ```shell
    tsdat ingest pipelines/example_pipeline/test/data/input/buoy.z06.00.20201201.000000.waves.csv
    ```

    If goes successfully it should output some text, ending with the line:

    ```shell
    Processing completed with 1 successes, 0 failures, and 0 skipped.
    ```

- Your shell can expand a glob to pass multiple paths. For example, to process all CSV
files in `data/to/process/` run:

    ```shell
    tsdat ingest data/to/process/*.csv
    ```

  By default, each matching pipeline runs once for each file. Add `--clump` to
  run each matching pipeline once with all files matching that pipeline; it does
  not combine unrelated inputs into a single pipeline or guarantee a single
  output file. Use `--multidispatch` if files should be processed by multiple
  matching pipelines. Files with no matching trigger are skipped.

- The `--help` option can be used to show additional usage information:

    ```shell
    tsdat ingest --help
    ```

### VAP Pipelines

- Value Added Product (VAP) Pipelines operate on the output of ingest pipelines. 

- The command to run these pipelines has a slightly different structure, where we enter the 
pipeline.yaml configuration file to use, as well as a start and end date:

    ```shell
    tsdat vap <pipeline/<pipeline-name>/config/pipeline.yaml> --begin yyyymmdd.HHMMSS --end yyyymmdd.HHMMSS
    ```

- The --help option can also be used here if you get stuck:

    ```shell
    tsdat vap --help
    ```

## Adding a new pipeline

1. From the project root, generate a new ingest or VAP pipeline:

    ```bash
    tsdat create-pipeline ingest
    # or: tsdat create-pipeline vap
    ```

    Follow the prompts; the new code will appear in `pipelines/<module_name>`.
    Generated VAP tests are skipped until you configure their upstream ingest inputs
    and expected output, then remove the skip to exercise the pipeline.
    `make cookies` (or `python templates/generate.py ingest`) is a compatible entry point
    for the same generator. Run `tsdat create-pipeline --help` for options.

2. See the README.md file inside that folder for more information on how to configure, run,
test, and debug your pipeline.

> This repository supports adding as many pipelines as you want - just repeat the steps above.

## Debugging Pipelines

The easiest way to develop and debug pipelines is using VSCode's built-in "Run and Debug" tool, 
accessed on the left-hand toolbar or ctrl-shift-d.

1. To set up a debug launcher, open the launch.json file located in the .vscode folder.

2. To add a debug script for an ingest pipeline, add and update:
    ```json
    {
        "name": "Debug Ingest",
        "type": "debugpy",
        "request": "launch",
        "module": "tsdat",
        "justMyCode": false,
        "console": "integratedTerminal",
        "args": [
            "ingest",
            "<path/to/datafile.ext>",
        ]
    },
    ```

    And for a VAP:

    ```json
    {
        "name": "Debug VAP",
        "type": "debugpy",
        "request": "launch",
        "module": "tsdat",
        "justMyCode": false,
        "console": "integratedTerminal",
        "args": [
            "vap",
            "pipelines/<pipeline_name>/config/pipeline.yaml",
            "--begin",
            "yyyymmdd.HHMMSS",
            "--end",
            "yyyymmdd.HHMMSS"
        ]
    },
    ```

3. Add breakpoint(s) where you want them, and now when you use VSCode's debugging tool, 
select the ingest or VAP pipeline launch configuration from the drop-down and hit the 
green arrow to start.

> Note that sometimes the debugger doesn't activate the appropriate conda environment, 
so you may have to cancel the debugger run, do so manually, then restart it again.

## Tutorials

Tutorial walkthroughs can be found on the Tsdat documentation online. It is recommended to 
start with the 
[ingest pipeline tutorial](https://tsdat.readthedocs.io/en/stable/tutorials/data_ingest/) 
and work through the rest of them on the website.

### List of tutorials:
- Ingest Pipeline: <https://tsdat.readthedocs.io/en/stable/tutorials/data_ingest/>
- Customizing Pipelines: <https://tsdat.readthedocs.io/en/stable/tutorials/pipeline_customization/>
- VAP Pipelines: <https://tsdat.readthedocs.io/en/stable/tutorials/vap_pipelines/>

- Using WSL in VSCode on Windows: <https://tsdat.readthedocs.io/en/stable/tutorials/setup_wsl/>
- Deploying Cloud Pipelines: <https://tsdat.readthedocs.io/en/stable/tutorials/aws_template/>

## Additional resources

- Learn more about `tsdat`:
  - GitHub: <https://github.com/tsdat/tsdat>
  - Documentation: <https://tsdat.readthedocs.io>
  - Data standards: <https://github.com/tsdat/data_standards>
- Learn more about `xarray`:
  - GitHub: <https://github.com/pydata/xarray>
  - Documentation: <https://xarray.pydata.org>
- Learn more about 'pydantic':
  - GitHub: <https://github.com/pydantic/pydantic/>
  - Documentation: <https://pydantic-docs.helpmanual.io>
- Other useful tools:
  - VS Code: <https://code.visualstudio.com/docs>
  - Docker: <https://docs.docker.com/get-started/>
  - `pytest`: <https://github.com/pytest-dev/pytest>
  - `black`: <https://github.com/psf/black>
  - `matplotlib` guide: <https://realpython.com/python-matplotlib-guide/>

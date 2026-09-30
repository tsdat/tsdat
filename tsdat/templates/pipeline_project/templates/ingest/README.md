# Ingest Pipeline Cookiecutter

This cookiecutter template is intended to be used to generate boilerplate pipeline code
to accelerate the development of ingest pipelines.

To use it, make sure you have `cookiecutter` installed (This should already be
installed if you set up your development environment according to
[the instructions](../../README.md#development-environment-setup))
```
pip install cookiecutter
```

Then run the following command from the top-level directory of the repository:
```
cookiecutter templates/ingest -o pipelines/
```

This will bring up a commandline interface which will go through some options. Consult
the documentation below for more information regarding these prompts.


| Field                    | Default               | Description                                                                                                                         |
|--------------------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `ingest_name`            | `Name of the Ingest`  | A short name / title for the ingest. It is used as a title in the README and it is also used to generate the pipeline folder name.  |
| `ingest_location`        | `Location`            | The name of the location where the instrument or ingest is located.                                                                 |
| `ingest_description`     | `Brief description of the ingest` | A brief description of the ingest. Used in the README and in the dataset configuration file as a global attribute.      |
| `use_custom_data_reader` | `no`                  | Flag to generate a custom DataReader. Use this if data cannot be read in using a built-in tsdat DataReader.                         |
| `use_custom_data_converter` | `no`               | Flag to generate a custom DataConverter. Use this to apply custom conversions to your input data as it is being retrieved.          |
| `use_custom_qc`          | `no`                  | Flag to generate a custom QualityChecker and QualityHandler. Use this if you want to apply custom quality checks or handlers.       |

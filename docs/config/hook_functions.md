# Pipeline Code Hooks

Each pipeline base class provides certain methods which the developer
can override if desired to customize pipeline functionality. In your
template repository, your Pipeline class will come with all the hook
methods stubbed out automatically (i.e., they will be included with an
empty definition).

The following hook methods (which can be easily identified because they
all start with the \'hook\_\' prefix) are provided in the pipeline
template found in the `pipelines/<ingest_name>/pipeline.py` file. They
are listed in the order that they are executed (see image in
`Configuring Tsdat<configuring_tsdat>`{.interpreted-text role="ref"}).

::: {.autosummary nosignatures=""}
\~tsdat.pipeline.pipelines.IngestPipeline.hook_customize_dataset
\~tsdat.pipeline.pipelines.IngestPipeline.hook_finalize_dataset
\~tsdat.pipeline.pipelines.IngestPipeline.hook_plot_dataset
:::

The plotting hook (`hook_plot_dataset`) is likely to be the most useful
for users. This hook creates plots and saves them to the storage
directory with the output dataset and is a good way to check the
pipeline output. Below is shown a custom plotting example:

```python
def hook_plot_dataset(self, dataset: xr.Dataset):
    # DEVELOPER: (Optional, recommended) Create plots.
    location = self.dataset_config.attrs.location_id
    datastream: str = self.dataset_config.attrs.datastream

    date, time = get_start_date_and_time_str(dataset)

    fig, ax = plt.subplots()
    dataset["example_var"].plot(ax=ax, x="time")  # type: ignore
    fig.savefig(self.get_ancillary_filepath(title="example_plot"))
    plt.close(fig)
```

::: {.autoclass members="hook_customize_dataset, hook_finalize_dataset, hook_plot_dataset" noindex=""}
tsdat.pipeline.pipelines.IngestPipeline
:::

# Pipeline Code Hooks

Each pipeline base class provides certain methods which the developer can override if desired to customize pipeline
functionality. In your template repository, your Pipeline class will come with all the hook methods stubbed out
automatically (i.e., they will be included with an empty definition).

The following hook methods (which can be easily identified because they all start with the **`hook_`** prefix) are
provided in the [pipeline-template](https://github.com/tsdat/pipeline-template) found in the
`pipelines/<ingest_name>/pipeline.py` file.

1. `hook_customize_dataset`
2. `hook_finalize_dataset`
3. `hook_plot_dataset`

Code hooks are executed according to this image:

![Ingest pipeline function calls](../figures/tsdat_ingest_pipeline.png)

Most users will only want to customize the plotting hook (`hook_plot_dataset`). This hook creates plots and saves them
to the storage area. Below is a simple example:

```python
def hook_plot_dataset(self, dataset: xr.Dataset):
    with plt.style.context("shared/style.mplstyle"): # (1)!
        fig, ax = plt.subplots()
        dataset["example_var"].plot(ax=ax, x="time")  # type: ignore
        fig.savefig(self.get_ancillary_filepath(title="example_plot"))
        plt.close(fig)
```

1. *Optional*: You can use `plt.style.context()` with a `matplotlib` stylesheet or the path to your own stylesheet to
    temporarily clear other styles and set the styles / `rcParams` for your plots.

!!! note

    Be sure to use the `self.get_ancillary_filepath()` method to ensure that:

    * the plot filename is standardized according to your storage configurations
    * the pipeline knows to save this file when it syncs with the storage area later

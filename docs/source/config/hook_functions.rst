.. _hook_functions:


Pipeline Code Hooks
===================

Each pipeline base class provides certain methods which the developer can override if desired to customize
pipeline functionality. In your template repository, your Pipeline class will come with all the hook methods stubbed out
automatically (i.e., they will be included with an empty definition).

The following hook methods (which can be easily identified because they all start with the 
'hook\_' prefix) are provided in the pipeline template found in the ``pipelines/<ingest_name>/pipeline.py`` file.
They are listed in the order that they are executed (see image in :ref:`Configuring Tsdat<configuring_tsdat>`).

.. autosummary::
    :nosignatures:

    ~tsdat.pipeline.pipelines.IngestPipeline.hook_customize_dataset
    ~tsdat.pipeline.pipelines.IngestPipeline.hook_finalize_dataset
    ~tsdat.pipeline.pipelines.IngestPipeline.hook_plot_dataset
	
The plotting hook (``hook_plot_dataset``) is likely to be the
most useful for users. This hook creates plots and saves them to the storage
directory with the output dataset and is a good way to check the pipeline
output. Below is shown a custom plotting example:

.. code-block:: python

    def hook_plot_dataset(self, dataset: xr.Dataset):
        # DEVELOPER: (Optional, recommended) Create plots.
        location = self.dataset_config.attrs.location_id
        datastream: str = self.dataset_config.attrs.datastream

        date, time = get_start_date_and_time_str(dataset)

        with self.storage.uploadable_dir() as tmp_dir:

            fig, ax = plt.subplots()
            dataset["example_var"].plot(ax=ax, x="time")  # type: ignore
            fig.suptitle(f"Example Variable at {location} on {date} {time}")
            format_time_xticks(ax)
            plot_filepath = self.storage.get_ancillary_filepath(
                title="example_plot",
                extension="png",
                root_dir=tmp_dir,
                dataset=dataset,
            )
            fig.savefig(plot_filepath)
            plt.close(fig)


.. autoclass:: tsdat.pipeline.pipelines.IngestPipeline
    :members: hook_customize_dataset, hook_finalize_dataset, hook_plot_dataset
    :noindex:

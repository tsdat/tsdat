.. _storage_config:

Storage Configuration
=====================
The storage config file ``storage.yaml`` describes how the output dataset will be saved to persistent storage.
Specifically, it identifies the following two components:

#. **Storage Class** - the class that will be used to persist output dataset
#. **Handler** - the class that will be used to write datasets and read datasets back from the store

Each pipeline template will include a starter storage config file in the config folder.
It will work out of the box, but the configuration should be tweaked according to the
specifics of your pipeline.  Consult the :ref:`getting-started` section for more information on getting started with a template.

.. note::
   Tsdat templates come complete with a VS Code IDE configuration that will provide inline documentation and auto-complete
   for your yaml configuration files.  Consult the :ref:`tutorials` section for more information on editing your pipeline in
   VS Code.

Storage Classes
^^^^^^^^^^^^^^^^^^

Currently there are two provided storage classes:

#. ``FileSystem`` - saves to local filesystem
#. ``S3Storage`` - saves to an AWS S3 bucket (requires an AWS account with admin privileges)

Both classes save datasets to file, and the user can specify the output file format(s) via the ``handler`` parameter of
the config file.

.. note::
   The S3Storage class is meant to work with the AWS Pipeline Template which is currently being refactored and
   will be included in a subsequent release.

.. note::
   To implement custom storage, such as storing in a database, you can extend the ``tsdat.io.base.Storage`` class.

Handler Classes
^^^^^^^^^^^^^^^^^^

Handlers declare the class that should be used to write output datasets and to read datasets back from persistent storage.
The NetCDFHandler is the default handler, but you can add a custom handler to add additional file formats or to write
to a different storage medium such as a database.  The only requirement is that it can read and write to and from an
Xarray dataset.  Handlers must extend the ``DataHandler`` abstract class and encapsulate a DataReader and DataWriter
class, which should implement the following two methods, respectively:

.. code-block:: python

    @abstractmethod
    def read(self, input_key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        """-----------------------------------------------------------------------------
        Uses the input key to open a resource and load data as a xr.Dataset object or as
        a mapping of strings to xr.Dataset objects.

        In most cases DataReaders will only need to return a single xr.Dataset, but
        occasionally some types of inputs necessitate that the data loaded from the
        input_key be returned as a mapping. For example, if the input_key is a path to a
        zip file containing multiple disparate datasets, then returning a mapping is
        appropriate.

        Args:
            input_key (str): An input key matching the DataReader's regex pattern that
            should be used to load data.

        Returns:
            Union[xr.Dataset, Dict[str, xr.Dataset]]: The raw data extracted from the
            provided input key.

        -----------------------------------------------------------------------------"""


    @abstractmethod
    def write(self, dataset: xr.Dataset, **kwargs: Any) -> None:
        """-----------------------------------------------------------------------------
        Writes the dataset to the storage area. This method is typically called by
        the tsdat storage API, which will be responsible for providing any additional
        parameters required by subclasses of the tsdat.io.base.DataWriter class.

        Args:
            dataset (xr.Dataset): The dataset to save.

        -----------------------------------------------------------------------------"""
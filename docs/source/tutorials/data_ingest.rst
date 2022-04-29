.. _template repository: https://github.blog/2019-06-06-generate-new-repositories-with-repository-templates/
.. _Anaconda: https://www.anaconda.com/
.. _Windows Subsystem for Linux: https://docs.microsoft.com/en-us/windows/wsl/about

.. _data_ingest: 

Pipeline Template Tutorial
--------------------------

In this tutorial we will build a data pipeline to ingest global
marine data hosted by the National Oceanic and Atmospheric Administration’s 
(NOAA) National Centers for Environmental Information (NCEI). The data can be 
found at https://www.ncdc.noaa.gov/cdo-web/datasets under the “Global Marine 
Data” section.

We will walk through the following steps in this tutorial:

#.	Examine and download the data
#.	Set up a GitHub repository in which to build our ingestion pipeline
#.	Modify configuration files and ingestion pipeline for our NCEI dataset
#.	Run the ingest data pipeline on NCEI data

Now that we’ve outlined the goals of this tutorial and the steps that we will 
need to take to ingest this data we can get started with step #1. 

Examining and downloading the data
==================================

Navigate to https://www.ncdc.noaa.gov/cdo-web/datasets and download the 
documentation and a data sample from their global marine data section.

.. figure:: global_marine_data/global_marine_data_webpage.png
   :alt: NOAA / NCEI Webpage for Global Marine Data sample data and documentation.


The documentation describes each variable in the sample dataset and will be 
extremely useful for updating our configuration file with the metadata for this
dataset. The metadata we care most about are the units and user-friendly text 
descriptions of each variable, but we also need to be on the lookout for any 
inconsistencies or potential data problems that could complicate how we process
this dataset. Take, for example, the following descriptions of the various 
temperature measurements that this dataset contains and note that the units are
not necessarily the same between files in this dataset:

.. figure:: global_marine_data/global_marine_data_documentation.png
   :alt: Global Marine Data documentation snippet indicating temperature measurements can be reported in Celcius or Fahrenheit depending on contributor preference.


If we were collecting this data from multiple users, we would need to be aware 
of possible unit differences between files from different users and we would 
likely want to standardize the units so that they were all in Celsius or all in
Fahrenheit (Our preference is to use the metric system wherever possible). If 
we examine this data, it appears that the units are not metric – how 
unfortunate. Luckily, this is something that can easily be fixed by using 
tsdat.

.. figure:: global_marine_data/global_marine_data_csv_snippet.png
    :alt: Snippet from a sample data file.

    Selection from the sample dataset. It appears that units are recorded in the imperial system instead of the metric system – Sea Level Pressure is recorded in Hg instead of hPa (Hectopascal) and Air Temperature is recorded in degF (Fahrenheit) instead of degC (Celsius).


Creating a repository from a template
=====================================

Now that we have the data and metadata that we will need, let’s move on to 
step #2 and set up a GitHub repository for our work. What we are looking to 
do is read in the NCEI “raw” data, apply variable names and metadata, 
apply quality control, and convert it into the netCDF format – an ‘ingest
pipeline’, in other words. To do this, navigate to 
https://github.com/tsdat/pipeline-template 
and click “Use this template” (you must log into github to see this button).

.. figure:: global_marine_data/intro1.png
    :alt:


This will open https://github.com/tsdat/pipeline-template/generate (you can
also just open this link directly) which will prompt you to name your 
repository, as well as to make it public or private.

.. figure:: global_marine_data/intro2.png
    :alt:
  
    Example shown is titled "ncei-global-marine-data-ingest".


Click “Create repository from template” to create your own repository that you 
can work in for this example.

Go ahead and clone the repository to your local machine and open it up in 
whatever IDE you prefer.


Set up Python
=============

Next, install Python 3.8+ if you haven’t already done so and create an 
environment in which to manage your project’s dependencies. You can download 
and install Python here: https://www.python.org. 

When developing with intent to deploy to a production system on Windows, we 
recommend managing your environment with `Anaconda`_ and/or using `Windows Subsystem
for Linux`_ (WSL). 

I go over a tutorial to set up WLF in :ref:`setting_up_wsl`, and will use WSL as
my working environment and VSCode as my IDE for the rest of this tutorial. 
The environment or container in use will not affect the next steps.


Run the Basic Template
======================
If using VSCode, open the "Explorer" tab to see folder contents 
for the next step:

  .. figure:: global_marine_data/intro3.png
      :align: center
      :width: 100%
      :alt:

A few quick things on VSCode: in the left-hand toolbar, we will use the "Explorer", "Search", "Testing", and "TODO tree" icons in this tutorial. Also useful to know are the commands "ctrl \`" (toggle the terminal on/off) and "ctrl shift P" (open command search bar).

Start by opening a VSCode terminal with "ctrl \`" and installing the pipeline 
required packages::

    pip install -r requirements.txt


Navigate to the "runner.py" file and run::

    python runner.py  pipelines/example_pipeline/test/data/input/buoy.z06.00.20201201.000000.waves.csv
    
This will run the example pipeline provided in the "pipelines" folder in the template. 
All pipelines that we create are stored in the "pipelines" folder and are run using 
`python runner.py <path_to_data>`. 

Addition options for the runner can be queried by typing `python runner.py --help`.

  .. figure:: global_marine_data/intro4.png
      :align: center
      :width: 100%
      :alt:

  |

After the code runs, notice that a new ``storage/`` folder is created with the following contents:

  .. figure:: global_marine_data/intro5.png
      :align: center
      :width: 100%
      :alt:

  |

These files contain the outputs of the example pipeline. Note that there 
are two subdirectories here – "data" and "ancillary". "Data" contains the 
output data in either netcdf or csv format (specified by the user), and 
"ancillary" holds optional plots that a user can create. 

Note, the data directory name contains a “.a1” key.
This ending is called the “data level” and indicates the level of processing 
of the data. “00” represents raw data that has been renamed according 
to the data standards that tsdat was developed under, "a1" refers to data
that has been standardized and some quality control, and “b1” 
represents data that has been ingested, standardized, quality-controlled,
and contains added value from further analysis if applicable.

For more information on the standards used to develop tsdat, please consult 
`our data standards <https://github.com/tsdat/data_standards>`_.


Creating a New Ingest
=====================
Now let’s start working on ingesting the NCEI data.

1. In the Explorer window pane you'll see a list of all folders and files in this ingest -> right click on the top level README.md and select "open preview". The steps in this readme we are more or less following in this tutorial.

  .. figure:: global_marine_data/intro6.png
      :align: center
      :width: 100%
      :alt:

  |

2. Before starting, we'll run a quick test of the pipeline to make sure everything is set up properly. Navigate to "Testing" and run all tests using the "Play" icon by hoving over the "ingest" dropdown. Tsdat will automatically configure these tests, and they all should pass at this point in time, as indicated by green checkmarks.

  .. figure:: global_marine_data/intro7.png
      :align: center
      :width: 100%
      :alt:

  |

4. Navigate back to the "Explorer" pane and hit "ctrl \`" to open the terminal. 
Create a new ingest by running a python template creator called "cookiecutter" 
in the terminal using:
	
.. code-block::

    make cookies

There will follow a series of prompts that'll be used to auto-fill the new ingest. Fill
these in for the particular dataset of interest. For this ingest we will not be using 
custom QC functions, filereaders/writers, or converters, so select no for those as well. 
(See :ref:`Custom QC & file handler tutorial <pipeline_customization>` for those)

  .. figure:: global_marine_data/intro8.png
      :align: center
      :width: 100%
      :alt:

  |

Once you fill that list out and hit the final enter, Tsdat will create a new ingest folder 
named with the "module" name (ncei_arctic_cruise_example):

  .. figure:: global_marine_data/intro9.png
      :align: center
      :width: 100%
      :alt:

  |

5. Right-click the README.md in our new "ncei_arctic_cruise_example" ingest and 
"open-preview". Scroll down to "Customizing your pipeline" (we have already
accomplished the previous steps, but these are good to check).

We are now looking at step #1: Use the "TODO tree" extension or use the search tool
to find occurances of "# TODO-Developer".

  .. figure:: global_marine_data/intro10.png
      :align: center
      :width: 100%
      :alt:

  |

6. The "TODO tree" lists every literal "TODO" instance in the code, and we are looking
in particular for "TODO - Developer". (The "TODO tree" is in fact the oak tree icon in 
the left-hand window pane).

You may need to reload VS Code for these to show up in the ingest. Hitting "ctrl shift P"
on the keyboard to open the search bar, and type in and run the command "Reload Window".

  .. figure:: global_marine_data/intro11.png
      :align: center
      :width: 100%
      :alt:

  |

After doing the window reloads, all the newly created "TODOs" will show up in the new 
ingest folder.

  .. figure:: global_marine_data/intro12.png
      :align: center
      :width: 100%
      :alt:

  |

Customizing the New Ingest
==========================
Each ingest folder is particular to a specific data file, so we must customize our ingest
to our particular data file. The following section describes how to customize a pipeline 
for our historical ship data, following the TODOs list.

7. Let's start with "config/pipeline.yaml". 
    
The first line, "classname" in this file is the class path. This points to your 
"pipeline/pipeline.py" file, which contains the hook functions for the pipeline,
which we'll visit after setting up the input data and configuration files.

.. figure:: global_marine_data/intro13.png
    :alt:


8. The second line, "triggers", is expected naming convention for the input data.
The "regex" pattern here is expecting the filename to start with the "location_name" 
from the template creation questions. 

This regex pattern can be adjusted as the user or raw data requires, but for this case, 
let's rename the sample datafile to "arctic_ocean.sample_data.csv" and move it to
a new folder called "data" within our "ncei_arctic_cruise_example" directory.

.. figure:: global_marine_data/intro14.png
    :alt:


9. The third line, "retriever", is the first of two required user-customized configuration
files in "YAML" (Yet Another Markup Language) format, which we’ll need to modify to 
capture the variables and metadata we want to retain in this ingest.

Several tasks can be specified in the retriever file to apply to the input file:
    1. Specific file reader
    2. Rename data variables
    3. Applying conversions (timestamp format, unit conversion, basic calculations, etc)
    4. Mapping particular data variables by input file regex pattern
    
For this example, we will specify the file reader, rename the variables and initiate 
unit conversion. More complicated data conversion, like basic calculations,
can be accomplished by user-customized "Data Converters" (specify yes ("2") 
in the appropriate question after creating a new pipeline with `make cookies`). 
We won't go over task 3 in this tutorial.

.. figure:: global_marine_data/intro15.png
    :alt:

Replace the text in the "retriever.yaml" file with the following:

.. code-block:: yaml
  :linenos:
  
  classname: tsdat.io.retrievers.DefaultRetriever
  readers:
    .*:
      classname: tsdat.io.readers.CSVReader
      parameters: # Parameters to pass to CsvHandler. Comment out if not using.
        read_csv_kwargs:
          sep: ", *"
          engine: "python"
          index_col: False

  coords:
    time:
      # Mapping of regex pattern (matching input key/file) to input name & converter(s) to
      # run. The default is .*, which matches everything. Put the most specific patterns
      # first because searching happens top -> down and stops at the first match.
      .*:
        # The name of the input variable as returned by the selected reader. If using a
        # built-in DataReader like the CSVReader or NetCDFReader, then will be exactly the
        # same as the name of the variable in the input file.
        name: Time of Observation

        # Optionally specify converters to run. The one below converts string values into
        # datetime64 objects. It requests two arguments: format and timezone. Format is
        # the string time format of the input data (see strftime.org for more info), and
        # timezone is the timezone of the input measurement.
        data_converters:
          - classname: tsdat.io.converters.StringToDatetime
            format: "%Y-%m-%dT%H:%M:%S"
            timezone: UTC

  data_vars:
    latitude:
      .*:
        name: Latitude

    longitude:
      .*:
        name: Longitude

    pressure:
      .*:
        name: Sea Level Pressure
        data_converters:
          - classname: tsdat.io.converters.UnitsConverter
            input_units: hPa

    temperature:
      .*:
        name: Air Temperature
        data_converters:
          - classname: tsdat.io.converters.UnitsConverter
            input_units: degF

    dew_point:
      .*:
        name: Dew Point Temperature
        data_converters:
          - classname: tsdat.io.converters.UnitsConverter
            input_units: degF

    wave_period:
      .*:
        name: Wave Period

    wave_height:
      .*:
        name: Wave Height
        data_converters:
          - classname: tsdat.io.converters.UnitsConverter
            input_units: ft

    swell_direction:
      .*:
        name: Swell Direction

    swell_period:
      .*:
        name: Swell Period

    swell_height:
      .*:
        name: Swell Height
        data_converters:
          - classname: tsdat.io.converters.UnitsConverter
            input_units: ft

    wind_direction:
      .*:
        name: Wind Direction

    wind_speed:
      .*:
        name: Wind Speed
        data_converters:
          - classname: tsdat.io.converters.UnitsConverter
            input_units: cm/s



10. The fourth line in "pipeline.yaml", "dataset", refers to the "dataset.yaml"
configuration file. This file is where user-specified datatype and metadata are 
added to the raw dataset.

This part of the process can take some time, as it involves knowing or learning a lot 
of the context around the dataset and then writing it up succinctly and clearly so 
that your data users can quickly get a good understanding of what this dataset 
is and how to start using it. 

.. figure:: global_marine_data/intro16.png
    :alt:

Replace the text in the "dataset.yaml" file with the following. Note that the units
block is particularly important, and that variable names match between "retriever.yaml"
and "dataset.yaml".

.. code-block:: yaml
  :linenos:

  attrs:
    title: NCEI Arctic Cruise Example
    description: Historial marine data that are comprised of ship, buoy and platform observations.
    location_id: arctic_ocean
    dataset_name: ncei_arctic_cruise_example
    data_level: a1
    # qualifier: 
    # temporal: 
    # institution: 

  coords:
    time:
      dims: [time]
      dtype: datetime64[s]
      attrs:
        units: Seconds since 1970-01-01 00:00:00
        
  data_vars:
    latitude:                 # Name of variable in dataset
      dims: [time]            # Dimension of variable
      dtype: float            # Datatype
      attrs:
        long_name: Latitude   # Used in plots
        units: deg N          # Necessary for unit conversion and user understanding
        comment: ""           # Add a comment or description if necessary
        _FillValue: 99        # Bad data marker in raw dataset, typically -999
        fail_range: [-90, 90] # Expected failure range for "CheckFailMax"/Min" QC tests
        
    longitude:
      dims: [time]
      dtype: float
      attrs:
        long_name: Latitude
        units: deg N
        comment: ""
        
    pressure:
      dims: [time]
      dtype: float
      attrs:
        long_name: Pressure at Sea Level
        units: dbar
        comment: ""
        
    temperature:
      dims: [time]
      dtype: float
      attrs:
        long_name: Air Temperature
        units: degC
        comment: ""
        
    dew_point:
      dims: [time]
      dtype: float
      attrs:
        long_name: Dew Point
        units: degC
        comment: ""
        
    wave_period:
      dims: [time]
      dtype: float
      attrs:
        long_name: Wave Period
        units: s
        comment: Assumed to refer to average wave period
        _FillValue: 99
        warn_range: [0, 22] # Expected range for "CheckWarnMax"/Min" QC tests
        
    wave_height:
      dims: [time]
      dtype: float
      attrs:
        long_name: Wave Height
        units: m
        comment: Assumed to refer to average wave height
        
    swell_direction:
      dims: [time]
      dtype: float
      attrs:
        long_name: Swell Direction
        units: deg from N
        comment: Assumed to refer to peak wave direction
        fail_range: [0, 360]
        
    swell_period:
      dims: [time]
      dtype: float
      attrs:
        long_name: Swell Period
        units: s
        comment: Assumed to refer to peak wave period
        warn_range: [0, 22]
        
    swell_height:
      dims: [time]
      dtype: float
      attrs:
        long_name: Swell Height
        units: m
        comment: Assumed to refer to significant wave height
        
    wind_direction:
      dims: [time]
      dtype: float
      attrs:
        long_name: Wind Direction
        units: deg from N
        comment: ""
        fail_range: [0, 360]
        
    wind_speed:
      dims: [time]
      dtype: float
      attrs:
        long_name: Wind Speed
        units: m/s
        comment: ""
        

11. The last two lines in "pipeline.yml" are "quality" and "storage". These are located
in the "shared" folder in the top-level directory. The "quality.yml" file contains the
QC functions that we will run on this code, and the "storage.yml" file contains the 
path to the output file writer.

.. figure:: global_marine_data/intro17.png
    :alt:
    
The quality.yml file contains a number of built-in tsdat quality control functions,
which we will use as is for this ingest.

Quality control in tsdat is broken up into two types of functions: 'checkers' and 
'handlers'. Checkers are functions that perform a quality control test (e.g. check 
missing, check range (max/min), etc). Handlers are functions that do something with
this data.

.. figure:: global_marine_data/intro18.png
    :alt:
    
File output is handled by "storage.yml", and built-in output writers are to NETCDF4
file format or CSV.

.. figure:: global_marine_data/intro19.png
    :alt:
    
I won't do this here, but CSV output can be added by replacing the "handler" block in 
"storage.yml" with::

    handler:
      classname: tsdat.io.handlers.CSVHandler
      

12. Finally "pipeline.py" is the last "get pipeline to working mode" "TODO" we should
finish setting up here. As mentioned previously, it contains a series of hook 
functions that can be used along the pipeline for further data organization.

.. figure:: global_marine_data/intro20.png
    :alt:
   

We shall set up "hook_plot_dataset", which plots the processed data and save the 
figures in the storage/ancillary folder. To keep things simple,
only the pressure data is plotted here, but it's easy to switch or add variables
to this code template as desired:

.. code-block:: python
  :linenos:

  import xarray as xr
  import cmocean
  import matplotlib.pyplot as plt

  from tsdat import IngestPipeline, get_start_date_and_time_str, get_filename
  from utils import format_time_xticks


  class NceiArcticCruiseExample(IngestPipeline):
      """---------------------------------------------------------------------------------
        NCEI ARCTIC CRUISE EXAMPLE INGESTION PIPELINE
        
        "Historical marine data are comprised of ship, buoy, and platform observations."
      ---------------------------------------------------------------------------------"""

      def hook_customize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
          # (Optional) Use this hook to modify the dataset before qc is applied
          return dataset

      def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
          # (Optional) Use this hook to modify the dataset after qc is applied
          # but before it gets saved to the storage area
          return dataset

      def hook_plot_dataset(self, dataset: xr.Dataset):
          location = self.dataset_config.attrs.location_id
          datastream: str = self.dataset_config.attrs.datastream

          date, time = get_start_date_and_time_str(dataset)

          plt.style.use("default")  # clear any styles that were set before
          plt.style.use("shared/styling.mplstyle")

          with self.storage.uploadable_dir(datastream) as tmp_dir:

              fig, ax = plt.subplots()
              dataset["pressure"].plot(ax=ax, x="time", c=cmocean.cm.deep_r(0.5))
              fig.suptitle(f"Pressure Observations from at {location} on {date} {time}")
              format_time_xticks(ax)

              plot_file = get_filename(dataset, title="example_plot", extension="png")
              fig.savefig(tmp_dir / plot_file)
              plt.close(fig)


Running the Pipeline
====================

We can now re-run the pipeline using the "runner.py" file as before with::

    python runner.py pipelines/ncei_arctic_cruise_example/data/arctic_ocean.example_data.csv

  .. figure:: global_marine_data/intro21.png
      :align: center
      :width: 100%
      :alt:

  |

Once the pipeline runs, if you look in the "storage" folder, you'll see 
the plot as well as the netCDF file output (or csv if you changed the output writer earlier):

  .. figure:: global_marine_data/intro22.png
      :align: center
      :width: 100%
      :alt:

  |

Data can be viewed by opening the terminal (``ctrl ```) and running a quick python shell:

.. code-block:: bash

  # cd storage/root/data/arctic_ocean.ncei_arctic_cruise_example.a1
  # python
  
In the python shell that opens, we can view the dataset for a quick overview:

.. code-block::

  >>> import xarray as xr
  >>> ds = xr.open_dataset('arctic_ocean.ncei_arctic_cruise_example.a1.20150112.000000.nc')
  >>> ds
  <xarray.Dataset>
  Dimensions:             (time: 55)
  Coordinates:
    * time                (time) datetime64[ns] 2015-01-12 ... 2015-01-31T12:00:00
  Data variables: (12/24)
      latitude            (time) float64 ...
      longitude           (time) float64 ...
      pressure            (time) float64 ...
      temperature         (time) float64 ...
      dew_point           (time) float64 ...
      wave_period         (time) float64 ...
      ...                  ...
      qc_wave_height      (time) int32 ...
      qc_swell_direction  (time) int32 ...
      qc_swell_period     (time) int32 ...
      qc_swell_height     (time) int32 ...
      qc_wind_direction   (time) int32 ...
      qc_wind_speed       (time) int32 ...
  Attributes:
      title:         NCEI Arctic Cruise Example
      description:   Historial marine data that are comprised of ship, buoy and...
      location_id:   arctic_ocean
      dataset_name:  ncei_arctic_cruise_example
      data_level:    a1
      datastream:    arctic_ocean.ncei_arctic_cruise_example.a1
      history:       Ran by jmcvey3 at 2022-04-29T15:31:32.055678



Pipeline Tests
==============

The final TODOs listed are for adding detail to the pipeline description and for testing. Testing is best completed as a last step, after everything is set up and the pipeline outputs
as expected. If running a large number of datafiles, a good idea is to input one of those datafiles here, along with its expected output, and have a separate data folder to collect input files.

.. figure:: global_marine_data/intro23.png
    :alt:

Move  the input and output files to the test/data/input/ and test/data/expected/ folders,
respectively for the test to pass.

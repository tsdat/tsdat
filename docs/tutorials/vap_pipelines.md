# Value Added Product (VAP) Pipeline Tutorial

In this tutorial, we'll go over how to create Value Added Product (VAP) pipelines using
Tsdat. The purpose of a VAP pipeline is to conduct additional processing and/or analysis
on top of data that has been through ingest pipelines. This can range from something as 
simple as combining datasets produced in the ingest pipelines; improved quality control
that requires variables from multiple datasets; bin-averaging or custom analysis 
algorithms; to calculating new qualities from raw inputs (e.g. calculating salinity from
independent measurements of temperature and conductivity).

In this tutorial, we'll run through a data pipeline workflow that takes in raw CSV files
measured by a Sofar Spotter wave buoy and combine them into a final product that 
includes quality-controlled and standardized wave statistics, buoy location, and sea 
surface temperature. The source code for this workflow is located at 
https://github.com/ME-Data-Pipeline-Software/sofar_spotter_pipelines.

Before we start, Windows users should note that they need to use Windows Subsystem for
Linux (WSL) to run VAP pipelines, and have conda install on their chosen Linux 
distribution. Instructions for installing WSL are located [here](./setup_wsl.md).

TODO: Add data and link to MHKDR, add setup errors section, table of contents and what covers what

## Installing the Pipeline Repository
We'll begin by cloning or downloading the `sofar_spotter_pipelines` repository. There
are 4 pipelines in this repository:
  1. "spotter": The ingest pipeline that ingests the raw Spotter CSV files 
  (those ending in _FLT, _LOC, _SST) and runs quality control.
  2. "vap_gps": A VAP pipelines takes the raw GPS data and bin-averages it into 10 minute
  intervals. It is included for the purposes of this tutorial.
  3. "vap_wave_raw": A VAP pipeline that runs spectral analysis on the ingested buoy 
  motion data to calculate wave statistics and runs quality control. Meant to be run on 
  a daily interval.
  4. "vap_wave_stats": The final VAP pipeline that takes the wave statistics from 
  "vap_wave_raw" and interpolates GPS positions and sea surface temperature (SST) 
  measurements onto the wave timestamps. Also does some additional calculations and 
  summary plots. Meant to be run on a monthly interval.

The instructions for running the repository are located in the top-level README, and 
these are the same as in the [ingest pipeline tutorial](./data_ingest.md).

If you haven't already, create a new environment for running pipelines by opening a new
terminal in VSCode and running `conda env create`. If you are not on a Unix machine or 
are not on Windows WSL, this will error out, as one of the dependencies is built in C 
for Unix.

## Creating a New VAP

We can create a new VAP pipeline by running `make cookies` from the terminal, and 
entering "vap" in the first prompt.

You will then run through the same set of prompts as you do in the ingest pipeline. You 
may want to use the same parameters as the ingest pipeline you want to read from.

```txt
Please choose a type of pipeline to create [ingest/vap] (ingest): 
vap
What title do you want to give this ingest?: 
GPS Location
What label should be used for the location of the ingest? (E.g., PNNL, San Francisco, etc.): 
clallam
Briefly describe the ingest: 
GPS location measured by a Sofar Spotter wave buoy deployed in Clallam Bay, WA
Data standards to use with the ingest dataset ['basic','ACDD','IOOS']: 
basic
Do you want to use a custom DataReader? [y/N]: 
n
Do you want to use a custom DataConverter? [y/N]: 
n
Do you want to use a custom QualityChecker or QualityHandler? [y/N]: 
n
'vap_gps_location' will be the module name (the folder created under 'pipelines/') Is this OK?  [Y/n]: 
n
What would you like to rename the module to?:
vap_gps
'VapGpsLocation' will be the name of your TransformationPipeline class (the python class containing your custom python code hooks). Is this OK? [Y/n]: 
n
What would you like to rename the pipeline class to?
VapGPS
'clallam' will be the short label used to represent the location where the data are collected. Is this OK?  [Y/n]: 
y
```

This will generate a new pipeline called "vap_gps", which has already been done for this 
repository. If you open up this pipeline, you can see that the pipeline structure for 
the VAP is very much the same as the ingest pipeline, with a few key differences in the 
configuration files.

## The Spotter Ingest Pipeline
I'll go over briefly the ingest pipeline so we know what we're working with for the VAP
pipelines to follow.

This ingest is built so that all raw Spotter files can be run from a single command,
i.e.
```bash
python runner.py ingest <path/to/raw_spotter_files/*.CSV>
```
will automatically go through all of the downloaded CSV files and pick out the relevant
ones for the ingest: buoy motion ("_FLT.CSV", "_LOC.CSV", "_SST.CSV"). This pipeline is
compatible with Spotter2 and Spotter3 raw files.

The FLT pipeline runs using tsdat's built-in CSV reader and then conducts quality control
in the form of despiking and subsequently checking expected minimums and maximums. The
despiking algorithm is the Goring and Nikora 2002 phase state space algorithm, and 
spikes are replaced via a cubic polynomial with the 12 surrounding datapoints. After
checking spikes, values beyond +/- 3 m are removed. This range should be updated for 
expected sea states. Plots are created of these XYZ measurements.

The LOC pipeline runs using a custom CSV reader that is a copy of the built-in version,
though with specific handling for the latitude and longitude variables. Quality control
is run to check valid min/max values, and a plot is created of the GPS position.

The SST pipeline also runs using a custom CSV reader with a different caveat. A 
timestamp is not saved to this file; rather the system logs a value in milliseconds. 
This millisecond value is also recorded in the corresponding (same-numbered) FLT file,
which is read in to backtrack down the appropriate timestamp. Quality control is run
to verify the temperature measurements are between 0 and 40 C, and a timeseries plot is 
saved.

Each of these pipelines saves "a1" level data in netCDF format, one for each raw file.
We will go over the "vap_gps" pipeline next to run through setting up a basic VAP pipeline
configuration.

## GPS Averaging Pipeline

This particular VAP takes the ingest timeseries data, bins the data and averages it at
10 minute intervals (bin-averaging). GPS data is used here, but this example pertains 
to any timeseries.

We'll go over the key differences between the ingest and VAP pipelines setups, 
starting with how to run a VAP on the command line, then running through
the configuration files, and finally looking at the pipeline class in pipeline.py. 
While DataReaders are no longer necessary for VAPs, DataConverters, quality control, 
and file storage have not changed.

The VAP run command is set up differently from the ingest, where we specify the path 
to the VAP pipeline configuration file we want to run, as well as a "begin" time and an
"end" time to span the time region of data we want to collate.
```bash
python runner.py vap <path/to/vap/pipeline.yaml> --begin <yyyymmdd.HHMMSS> --end <yyyymmdd.HHMMSS>
```
For the purposes of this tutorial, we'll run a single day at the start of Aug, 2021.
```bash
python runner.py vap pipelines/vap_gps/pipeline.yaml --begin 20230801.000000 --end 20230802.000000
```

Assuming you ran the ingest pipeline given the ingest run command above, running this 
line will add the file
`storage/root/data/clallam/clallam.gps.b1/clallam.gps.b1.20210801.000000`. Note that our
"begin" timestamp is equivalent to the filename timestamp.

### Dataset Configuration

Our `dataset.yaml` file looks very much the same as the ingest pipeline. 
The configuration is copy-pasted from pipelines/spotter/dataset_loc.yaml, but with a 
few geospatial attributes added that define the maximum and minimum latitude and 
longitude values that a user can expect to see. 
In this case, these were found by manually looking at each variable and define 
the limits of the Spotter buoy's watch circle. They are also used later to set the 
limits in the plot hook.

```yaml
attrs:
  title: GPS Location
  description:
    GPS location measured by a Sofar Spotter wave buoy deployed in Clallam Bay, WA
  location_id: clallam
  dataset_name: gps
  data_level: b1
  geospatial_lat_min: 48.2735
  geospatial_lat_max: 48.2742
  geospatial_lat_units: degrees_north
  geospatial_lon_min: -124.2870
  geospatial_lon_max: -124.2852
  geospatial_lon_units: degrees_east

coords:
  time:
    dims: [time]
    dtype: datetime64[ms]
    attrs:
      units: Seconds since 1970-01-01 00:00:00 UTC
      long_name: Time
      standard_name: time
      timezone: UTC

data_vars:
  lat:
    dims: [time]
    dtype: float32
    attrs:
      units: degrees_north
      long_name: Latitude
      standard_name: latitude
      valid_min: -90
      valid_max: 90
  lon:
    dims: [time]
    dtype: float32
    attrs:
      units: degrees_east
      long_name: Longitude
      standard_name: longitude
      valid_min: -180
      valid_max: 180
```

### Pipeline Configuration

The pipeline configuration file, `pipeline.yaml`, has changed slightly, i.e. there is 
an additional "datastreams" parameter and "triggers" is now empty. 
The datastreams parameter covers all of the pipeline outputs that generate variables 
one wants to retrieve for the VAP pipeline. 
In this case, we just want the "clallam.spotter-gps-1min.a1" data.

```yaml
classname: pipelines.vap_gps.pipeline.VapGPS
parameters:
  datastreams:
    - clallam.spotter-gps-1min.a1

triggers: []

retriever:
  path: pipelines/vap_gps/config/retriever.yaml

dataset:
  path: pipelines/vap_gps/config/dataset.yaml

quality:
  path: shared/quality.yaml
storage:
  path: shared/storage.yaml
```

### Retriever Configuration

The retriever configuration file, `retriever.yaml`, has a number of addtions from the 
ingest version. The `DataReader` class is replaced for tsdat's built-in 
`StorageRetriever`, which accesses the storage file location to collect files containing
the datastreams that we specify in `pipeline.yaml`. In order for a VAP to work with the 
appropriate ingest pipelines, they all must use the same `storage.yaml` file located in 
the "shared" folder.

The coordinates and data variables also have additional parameters, "time" in 
particular. Sometimes it's desirable (real-time measurements) to have measurements that 
are aligned on a gridded time variable, rather than a random, instrument-dependent 
offset. 
For this VAP, a new timegrid is created that is spaced on a 10-minute interval. We do 
this by ignoring the file input regex, setting the name to "N/A", and setting the 
`CreateTimeGrid` DataConverter. This TimeGrid will start at the top of the hour and 
continue at 10-minute intervals to the "end" time specified in the VAP run command. 

If we create a new timegrid, Tsdat assumes that one of the DataTransforms will be 
used. If a DataTransform is not used, Tsdat will assume the input timestamps are 
already on a TimeGrid.

```yaml
coords:
  time:
    name: N/A
    data_converters:
      - classname: tsdat.transform.CreateTimeGrid
        interval: 10min
```

The two variables we're pulling from the ingest pipeline are "lat" and "lon". 
It's good practice to specify the file regex (gps) with starting and ending breaks (.*)
on the first indent.
On the next indent, we'll specify the variable ingest "name" and "data_converters" key.

Note that all VAP functional is incorporated into the Tsdat workflow via DataConverters.

The DataConverter (DataTransform) classname we want to use is
 `tsdat.transform.BinAverage`. 
The other built-in options are `NearestNeighbor` and `Interpolate`.

```yaml
data_vars:
  lat:
    .*gps.*:
      name: lat
      data_converters:
        - classname: tsdat.transform.BinAverage
  lon:
    .*gps.*:
      name: lon
      data_converters:
        - classname: tsdat.transform.BinAverage
```

Next we need to specify parameters to run the transformer properly on these variables.
There are two sets of parameters that we can set for the DataTransforms, called
`fetch_parameters` and `transformation_parameters`:

```yaml
classname: tsdat.io.retrievers.StorageRetriever
parameters:
  fetch_parameters:
    # How far in time to look after the "end" timestamp (+), before the "begin"
    # timestamp (-), or both (none) to find filenames that contain needed data
    time_padding: -24h

  transformation_parameters:
    # Where the point lies in the coordinate bounds (CENTER, LEFT, RIGHT)
    alignment:
      time: CENTER

    # How far to look for the next available data point
    range:
      time: 60s

    # Width of the transformation
    width:
      time: 600s
```

### Transformation Parameters

Both of these sets of parameters are best explained by pictures. 
For the `BinAverage` transform, "width" defines the size of the averaging window 
(600s = 10 min) and "alignment" defines the location of the window in respect to each 
timestamp. 

In the window shown below, the alignment is technically set to "LEFT". No matter what 
"alignment" is set to, the TimeGrid will always start at 00:00. For instance, if 
"aligment" is set to "CENTER" and the width is 600 s, the 01:00:00 timestamp represents 
bin-averaged data between 00:55:00 and 01:05:00. Tsdat will attempt to fetch the file
that contains data for the previous 5 minutes here. If a filename with a timestamp 10 
minutes prior cannot be found, the 01:00:00 timestamp will be set to nan.

The "range" keyword is relevant for the `NearestNeighbor` and `Interpolate` transforms,
and defines how far from the last timestamp to search for the next measurement. The
"range" and "width" parameters should be set in seconds.

![transform_params](vap/tranform_params.png)


### Fetch Parameters

The "time_padding" fetch parameter can be critical to set correctly. To show what this
parameter does, open up a new terminal and run if you haven't already:
```bash
python runner.py vap pipelines/vap_gps/pipeline.yaml --begin 20230801.000000 --end 20230802.000000
```

When the pipeline completes successfully, navigate to 
`storage/root/ancillary/clallam/clallam.gps.b1/clallam.gps.b1.20210903.000000.timeseries.nc`
and look at the X-axis ticks. Notice that data is found for the entire day.

Now, set `timepadding: 0`, open a terminal and run the vap again:
```bash
python runner.py vap pipelines/vap_gps/pipeline.yaml --begin 20230801.000000 --end 20230802.000000
```
Check out the same .png file again. See that half the data is missing for the day. 

The image below depicts the idea of what this parameter does. Datafiles, particularly
these from the Spotter buoy, are not saved on any particular time schedule. When this is
the case, we want to set `time_padding` far enough so that Tsdat is able to retrieve 
all of the files that contain the data we want, but not so far as to significantly 
slow down the pipeline.

![fetch_params](vap/fetch_params.png)

Setting `timepadding: -24h` tells Tsdat to find files up to 24 hours earlier than the 
start date we specified. If we set `timepadding: +24h`, it will grab datafiles 24 hours 
after the "end" time. Setting `timepadding: 24h` will grab both before and after.
Units of hour ('h'), minute ('m'), seconds ('s'), and milliseconds ('ms') can be used 
here. If no units are specified, tsdat will default to using seconds.


### Pipeline Class Configuration
The final file that's important to review is the change to the pipeline class located
in `pipeline.py`. The first is the inherited class change from `IngestPipeline` to 
`TransformationPipeline`. The second is the addition of the 
`hook_customize_input_datasets` hook, which allows the user to alter datasets retrieved
from the ingest pipeline. The input/return here is a dictionary of datasets that are 
labeled based on their individual datastreams and "begin"/"end" timestamps. The other
three hooks have not changed.

```python
class VapGPS(TransformationPipeline):
    """---------------------------------------------------------------------------------
    This is an example pipeline meant to demonstrate how one might set up a
    pipeline using this template repository.

    ---------------------------------------------------------------------------------"""

    def hook_customize_input_datasets(self, input_datasets) -> Dict[str, xr.Dataset]:
        # Code hook to customize any input datasets prior to datastreams being combined
        # and data converters being run.
        return input_datasets

    def hook_customize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        # (Optional) Use this hook to modify the dataset before qc is applied
        return dataset

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        # (Optional) Use this hook to modify the dataset after qc is applied
        # but before it gets saved to the storage area
        return dataset

    def hook_plot_dataset(self, dataset: xr.Dataset):
        # (Optional, recommended) Create plots.
        pass
```


## Closing thoughts on the GPS VAP

With the information contained in the above sections, you can create most VAP 
pipelines. The rest of this tutorial reviews the other two VAPs in this 
repository, as well as a number of features/idiosyncracies you might need to know for 
your own particular use case. This are listed as the following

1. Adding new coordinates to a VAP dataset
2. Conducting analyses beyond that of bin-averaging, nearest neighbor, and interpolation
3. Creating a VAP dataset using the time coorinate from an ingest dataset
4. A VAP example that fetches multiple datastreams


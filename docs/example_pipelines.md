# Example Pipelines
The following list of pipeline repositories are currently built and display a broad 
range of pipeline workflows that can be built with tsdat. The following sections link to each
of these repositories and provide a description of what each pipeline in the repository
does. If you have a specific use case you want to build a pipeline for, it is recommended
that you see if a similar example is listed below to get started.

## MHKiT Example Pipelines
[MHKiT-Pipelines](https://github.com/ME-Data-Pipeline-Software/MHKiT-Pipelines)

The following list of pipelines are the standardized versions of the jupyter notebooks
provided in the MHKiT examples folder
1. *adcp_example* (Read data from an up-looking ADCP)
    - Implements custom DataReader with input parameters
    - Converts instrument timezone from local to UTC
    - Creates second storage handler to store MATLAB file in addition to native NetCDF
    - Creates timeseries plots of velocity, acoustic amplitude and correlation
2. *adv_example* (Read data from a Nortek ADV)
    - Implements custom DataReader with input parameters
    - Implements custom quality control checker and handler
    - Creates second storage handler to store MATLAB file in addition to native NetCDF
3. *cdip_example* (Pull NOAA CDIP wave data)
    - Implements custom DataReader that requests data from an online server
    - Implements custom DataReader to read .json files
    - Applies configuration file overrides depending on file trigger
    - Uses nonstandard pipeline trigger to call pipeline (`runner_cdip_example.py`)
    - Update dataset metadata in pipeline hook depending on filename
4. *loads_example* (Pull loads data from an excel file)
    - Implements tsdat built-in CSVReader with specific parameters
    - Implements custom DataConverter to convert excel timestamp to datetime64
    - Apply analysis functions in pipeline hook
    - Use custom plot functions in pipeline plot hook
5. *power_example* (Pull power data from a csv file)
    - Implements tsdat built-in CSVReader with specific parameters
    - Convert epoch time to datetime64
    - Apply analysis functions in pipeline hook
6. *river_example* (Pull USGS stream gauge data)
    - Implements custom DataReader that requests data from an online server
    - Implements custom DataReader to read .json files
    - Implements multiple configuration files
    - Implements tsdat built-in UnitsCoverter
7. *tidal_example* (Pulls NOAA Tides and Currents data)
    - Implements custom DataReader to read .json files
    - Removes non monotonically increasing timestamps in quality configuration file
    - Implements tsdat built-in UnitsConverter
    - Applies custom functions in pipeline hook
    - Sets up empty variable in dataset configuration file to be filled by pipeline hook
    - Implements custom plotting functions in pipeline plot hook
8. *wave_example* (PULL NOAA NDBC wave data)
    - Implements tsdat built-in ZipReader to read archived data file containing waves and power data
    - Implements tsdat built-in CSV reader with custom parameters
    - Implements custom data reader to request data from server
    - Shows how to specify which variables are pulled from which datafile
    - Implements tsdat built-in UnitsConverter and time converters for the time variables
    - Adds several variables in dataset.yaml that are filled in pipeline hook
    - Applies custom plots
9. *wecsim_example* (Pull WEC-Sim MATLAB data)
    - Implements custom DataReader to read version of WEC-Sim MATLAB output


## ADCP Example Pipelines
[acoustic_doppler_pipelines](https://github.com/ME-Data-Pipeline-Software/acoustic_doppler_pipelines)

This repository contains pipelines that process ADCP data. The vessel-mounted pipelines
are set to read in data, set the heading based off of GPS heading or the ADCP compass,
and correct velocity using GPS or bottom-track data. It also includes functionality
to calculate GPS velocity if it (VTG) wasn't recorded by the GPS. The up-looking ADCP
pipeline is a generic workflow for processing a bottom-mounted instrument.

These pipelines use several shared files and tsdat quality control checkers/handlers 
that are stored in the repository shared folder. All of these pipelines dataset 
configuration files are written to Integrated Ocean Observation System (IOOS) standards.

1. *dn_looking_adcp* (Reads and processes data from vessel-mounted Teledyne RDI ADCPs)
    - Implements custom DataReader with input parameters
    - Converts timestamps to datetime64 and from local to UTC timezones
    - Uses dataset attributes to determine functionality incorporated in pipeline hook
    - Applies custom processing/analysis in pipeline hook
    - Custom quality control with editable input parameters
2. *sigvm* (Reads and processes data from Nortek SignatureVM ADCPs)
    - Uses tsdat built-in ZipReader to read in .SigVM data archive and extract the 
    binary adcp file, GPS NMEA file, and ignore all other files
    - Converts timestamps to datetime64 and from local to UTC timezones
    - Uses dataset attributes to determine functionality incorporated in pipeline hook
    - Applies custom processing/analysis in pipeline hook
    - Custom quality control with editable input parameters
3. *up_looking_adcp* (Reads and processes data from bottom-mounted ADCPs)
    - Implements custom DataReader with input parameters
    - Uses a custom data reader that takes in 3 parameters
    - Custom quality control with editable input parameters


## Wave Buoy Example Pipelines
[sofar_spotter_pipelines](https://github.com/ME-Data-Pipeline-Software/sofar_spotter_pipelines)

This repository contains a collection of pipelines that form a workflow that takes raw 
Sofar Spotter wave files to a final datafile containing wave statistics, sea surface
temperature, and GPS data. All of these pipelines dataset 
configuration files are written to Integrated Ocean Observation System (IOOS) standards.

1. *spotter*
    - Reads in 3 file types with different configuration files
    - Shows how to specify which variables are pulled from which datafile
    - Implements two different custom DataReaders and the built-in tsdat CSVReader
    - Implements custom quality control
2. *vap_gps* (not critical to workflow but makes good VAP example)
    - Basic VAP
    - Creates a timegrid and bin-averages variables to the new timegrid
3. *vap_wave_raw*
    - Reads in single type of wave file and combines them to start and end time
    - Does not create a new timegrid
    - Adds a new data coordinate and empty data variables to dataset
    - Runs spectral analysis on combined file and stores data in empty variables
    - Rebuilds dataset on binned spectral time variable
4. *vap_wave_stats*
    - Reads in ingest and VAP output
    - Adds a new data coordinate and empty data variables to dataset
    - Uses the Interpolate transform function to map ingest data to VAP dataset

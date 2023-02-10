# from cds3 import cds_parse_transform_params

"""
Notes for transform wrapper

Parameters:
    1) Transformation params in PCM json format
       * Note that since we won't let users name coordinate systems because we only have one, everywhere in the PCM
        format where coordinate system name is mentioned, we should use the value 'coord_sys'.  i.e.,

        "coordinate_system_defaults": [
          {
            "name": "coord_sys",
            "dim": "time",
            "default_value": "AUTO"
          },
          {
            "name": "coord_sys",
            "dim": "range",
            "default_value": "TRANS_PASSTHROUGH"
          }
        ],


    2) Set of xarray datasets for the input datastreams
        * Note that if range transform parameters were set on any datastreams, the xarray datasets much have at least
         'range' amount of extra data retrieved before and after the day being processed (if it exists).
        * Note that we should run units converters BEFORE passing the input datasets to the transform.

        * Note that variables from the original input datasets need to have the name used in the output dataset.  So
            we can either rename input variables to match their output names BEFORE we pass them to this method OR
            we can leave the original names and pass mapping of input to output names to this method so we know
            which variables to grab when we call the cds_transform_driver method.
        ds1
          varA <-- retrieved name, not the name from the original file
          varB <-- retrieved name, not the name from the original file

        ds2
          varC <-- retrieved name, not the name from the original file
          varD <-- retrieved name, not the name from the original file

    3) Xarray dataset for the output.  It should have the coordinate variables set (We know this because the shape of
        the output will be defined in the tsdat config file.  Shape could be mapping or regular interval.)
        It should have placeholder variables created for all the data and qc variables (every data variable must have
        a qc variable created).  All the values can be missing or Nan - doesn't matter because transform code will overwrite them.

What this method does:
    Transform will transform data variable and qc variable values in place

Steps:
    1) Convert all of the xarray input datasets to CDSGroup objects.  We will need Groups for input datastream,
        obs (should be just one because we just have one file), and coordinate system

        Retrieved data structure
        Input datastream                                          CoordinateSystem
            obs 0 (all the data that came from file 1)
                CDSVar 1 (pointer to the coord sys)
                CDSVar 2 (pointer to the coord sys)
            obs 1 (all the data that came from file 2)

        * Note tsdat does not make users define a coordinate system name, so we will use 'coord_sys' or similar since
            we only allow 1 coordinate system per process.

    2) Convert the output xarray object into a CDS Group set of objects (same as above)

    3) Convert the transform parameters into a map of strings.  Key is the data stream or coordinate system name, the
       value is transform parameters string in ADI format.

    4) For each input datastream, see if there are transform params in the map.  If so, then we run
        cds_parse_transform_params on the input datastream CDSGroup and pass in the string.

    5) See if there is a coord_sys entry in the map, and if so, then run cds_parse_transform_params on the coordinate
        system CDSGroup and pass in the string.

    6) Iterate through the data variables and call  cds_transform_driver on them.
        int cds_transform_driver(CDSVar *invar, CDSVar *qc_invar, CDSVar *outvar, CDSVar *qc_outvar)

    7) Convert the output CDS structure back to Xarray format (same as what we do with adi_py, so we share the data
        pointer and we only have to copy over attributes that changed).


"""


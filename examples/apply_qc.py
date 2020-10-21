from tsdat import TimeSeriesDataset, Config, load_configs, apply_qc, save, load, FileFormat

# Load the data/qc config parameters
config: Config = load_configs(["data/qc.yml"])

# Read generic 1-d csv file into
# For this data column 0 is the time column, which is the only dimension
# In this data, UTC timestamp is specified as a long int
dataset: TimeSeriesDataset = load("data/test_data_bad_temp_too_low.csv", FileFormat.CSV, config, index_col=0)

# Now apply qc to the xarray dataset
apply_qc(config, dataset)

print(dataset.xr)

# Now save the dataset with qc values as a netcdf file
#
save(dataset, "data/test_output.nc", FileFormat.NETCDF)

# Now save the dataset with qc values to csv with a companion metadata file
save(dataset, "data/test_output.csv", FileFormat.CSV)

# Now reload the saved dataset with qc values in netcdf
dataset2: TimeSeriesDataset = load("data/test_output.nc", FileFormat.NETCDF)

# Plot the qc results so we can check our data
dataset2.plot_qc("temp_mean")

# Plot and save to a file
dataset2.plot_qc("temp_mean", base_filename="data/test_output")

# TODO: Now remove all values with bad QC (do later when we have a use case)
# dataset.filter_qc(type="bad") # bad, indeterminate, or all - use constants

# Make sure any file handles are closed (since xarray dynamically loads some file types)
dataset.close()
dataset2.close()







import act
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict
from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil

class CustomIngestPipeline(IngestPipeline):
    """-------------------------------------------------------------------
    This is an example class that extends the default IngestPipeline in
    order to hook in custom behavior such as creating custom plots.
    If users need to apply custom changes to the dataset, instrument
    corrections, or create custom plots, they should follow this example
    to extend the IngestPipeline class.
    -------------------------------------------------------------------"""
    def customize_raw_datasets(self, raw_dataset_mapping: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """-------------------------------------------------------------------
        Hook to allow for user customizations to one or more raw xarray Datasets
        before they merged and used to create the standardized dataset.  The
        raw_dataset_mapping will contain one entry for each file being used
        as input to the pipeline.  The keys are the standardized raw file name,
        and the values are the datasets.

        This method would typically only be used if the user is combining
        multiple files into a single dataset.  In this case, this method may
        be used to correct coordinates if they don't match for all the files,
        or to change variable (column) names if two files have the same
        name for a variable, but they are two distinct variables.

        This method can also be used to check for unique conditions in the raw
        data that should cause a pipeline failure if they are not met.

        This method is called before the inputs are merged and converted to
        standard format as specified by the config file.

        Args:
            raw_dataset_mapping (Dict[str, xr.Dataset])     The raw datasets to
                                                            customize.

        Returns:
            Dict[str, xr.Dataset]: The customized raw dataset.
        -------------------------------------------------------------------"""
        # In this hook we rename one variable from the surfacetemp file to
        # prevent a naming conflict with a variable in the conductivity file.
        for filename, dataset in raw_dataset_mapping.items():
            if "surfacetemp" in filename: 
                old_name = "Surface Temperature (C)"
                new_name = "surfacetemp - Surface Temperature (C)"
                raw_dataset_mapping[filename] = dataset.rename_vars({old_name: new_name})

            if "gill" in filename:
                name_mapping = {
                    "Horizontal Speed (m/s)":       "gill_horizontal_wind_speed",
                    "Horizontal Direction (deg)":   "gill_horizontal_wind_direction" 
                }
                raw_dataset_mapping[filename] = dataset.rename_vars(name_mapping)

        # No customization to raw data - return original dataset
        return raw_dataset_mapping

    def apply_corrections(self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Pipeline hook that can be used to apply standard corrections for the
        instrument/measurement or calibrations. This method is called
        immediately after the dataset is converted to standard format and
        before any QC tests are applied.

        If corrections are applied, then the `corrections_applied` attribute
        should be updated on the variable(s) that this method applies
        corrections to.

        Args:
            dataset (xr.Dataset):   A standardized xarray dataset where the
                                    variable names correspond with the output
                                    variable names from the config file.
        Returns:
            xr.Dataset: The input xarray dataset with corrections applied.
        -------------------------------------------------------------------"""
        # No corrections - return the original dataset
        return dataset

    def customize_dataset(self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Hook to allow for user customizations to the standardized dataset such
        as inserting a derived variable based on other variables in the
        dataset.  This method is called immediately after the apply_corrections
        hook and before any QC tests are applied.

        Args:
            dataset (xr.Dataset): The dataset to customize.

        Returns:
            xr.Dataset: The customized dataset.
        -------------------------------------------------------------------"""
        for raw_filename, raw_dataset in raw_dataset_mapping.items():
            if "currents" in raw_filename:
                num_bins = 50
                # Want to add Vel1 (mm/s), Vel2 (mm/s), Vel3 (mm/s), ... to the 
                # current_velocity variable so that it is two dimensional.
                vel_names = [f"Vel{i} (mm/s)" for i in range(1, num_bins + 1)]
                vel_data  = [raw_dataset[name].data for name in vel_names]
                vel_data  = np.array(vel_data).transpose()
                dataset["current_velocity"].data = vel_data

                # Want to add Dir1 (deg), Dir2 (deg), Dir3 (deg), ... to the 
                # current_direction variable so that it is two dimensional.
                vel_names = [f"Dir{i} (deg)" for i in range(1, num_bins + 1)]
                vel_data  = [raw_dataset[name].data for name in vel_names]
                vel_data  = np.array(vel_data).transpose()
                dataset["current_direction"].data = vel_data
        
        return dataset
    
    def create_and_persist_plots(self, dataset: xr.Dataset) -> None:
        """-------------------------------------------------------------------
        Hook to allow users to create plots from the xarray dataset after
        processing and QC have been applied and just before the dataset is
        saved to disk.

        To save on filesystem space (which is limited when running on the
        cloud via a lambda function), this method should only
        write one plot to local storage at a time. An example of how this
        could be done is below:

        ```
        filename = DSUtil.get_plot_filename(dataset, "sea_level", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            fig, ax = plt.subplots(figsize=(10,5))
            ax.plot(dataset["time"].data, dataset["sea_level"].data)
            fig.save(tmp_path)
            storage.save(tmp_path)

        filename = DSUtil.get_plot_filename(dataset, "qc_sea_level", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            fig, ax = plt.subplots(figsize=(10,5))
            DSUtil.plot_qc(dataset, "sea_level", tmp_path)
            storage.save(tmp_path)
        ```

        Args:
            dataset (xr.Dataset):   The xarray dataset with customizations and
                                    QC applied.
        -------------------------------------------------------------------"""
        for variable_name in dataset.data_vars.keys():
            if variable_name.startswith("qc_") or "time" not in dataset[variable_name].dims:
                continue
            filename = DSUtil.get_plot_filename(dataset, variable_name, "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                display = act.plotting.TimeSeriesDisplay(dataset, subplot_shape=(2,), figsize=(15,9), sharex=True)
                display.plot(variable_name, subplot_index=(0,))
                display.qc_flag_block_plot(variable_name, subplot_index=(1,))
                display.fig.savefig(tmp_path)
                plt.close()

                self.storage.save(tmp_path)
        
        # Custom plot for current velocity and direction
        filename = DSUtil.get_plot_filename(dataset, "current_by_depth", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            fig, axes = plt.subplots(nrows=2, figsize=(15,9))
            dataset.current_velocity.plot(ax=axes[0], x="time", y="depth", yincrease=False, snap=True)
            dataset.current_direction.plot(ax=axes[1], x="time", y="depth", yincrease=False, snap=True, cmap="hsv")
            fig.set_tight_layout(True)
            fig.savefig(tmp_path)
            plt.close()
            self.storage.save(tmp_path)

        return


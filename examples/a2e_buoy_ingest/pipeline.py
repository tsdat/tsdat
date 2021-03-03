import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

from typing import Dict
from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil

plt.style.use("./styling.mplstyle")

class BuoyIngestPipeline(IngestPipeline):
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

        def format_time_xticks(ax, /, *, start=4, stop=21, step=4, date_format="%H-%M"):
            ax.xaxis.set_major_locator(mpl.dates.HourLocator(byhour=range(start, stop, step)))
            ax.xaxis.set_major_formatter(mpl.dates.DateFormatter(date_format))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)
        
        def double_plot(ax, twin, /, *, data, colors, var_labels=["",""], ax_labels=["",""], **kwargs):
            def _add_lineplot(_ax, _data, _c, _label, _ax_label, _spine):
                _line = _data.plot(ax=_ax, c=_c, label=_label, linewidth=2, **kwargs)
                _ax.tick_params(axis="y", which="both", colors=_c)
                _ax.set_ylabel(_ax_label, color=_c)
                _ax.spines[_spine].set_color(_c)
            _add_lineplot(ax, data[0], colors[0], var_labels[0], ax_labels[0], "left")
            _add_lineplot(twin, data[1], colors[1], var_labels[1], ax_labels[1], "right")
            twin.spines["left"].set_color(colors[0])  # twin overwrites ax, so set color here
            lines = ax.lines + twin.lines
            labels = [line.get_label() for line in lines]
            twin.legend(lines, labels, ncol=len(labels), loc=1)

        # Useful variables
        ds = dataset
        date = pd.to_datetime(ds.time.data[0]).strftime('%d-%b-%Y')
        cmap = sns.color_palette("viridis", as_cmap=True)
        colors = [cmap(0.00), cmap(0.60)]

        # Create the first plot -- Surface Met Parameters
        filename = DSUtil.get_plot_filename(dataset, "surface_met_parameters", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

            # Define data and metadata
            data = [
                [ds.wind_speed, ds.wind_direction], 
                [ds.pressure, ds.rh], 
                [ds.air_temperature, ds.CTD_SST]]
            var_labels = [
                [r"$\overline{\mathrm{U}}$ Cup", r"$\overline{\mathrm{\theta}}$ Cup"],
                ["Pressure", "Relative Humidity"],
                ["Air Temperature", "Sea Surface Temperature"]]
            ax_labels = [
                [r"$\overline{\mathrm{U}}$ (ms$^{-1}$)", r"$\bar{\mathrm{\theta}}$ (degrees)"],
                [r"$\overline{\mathrm{P}}$ (bar)", r"$\overline{\mathrm{RH}}$ (%)"],
                [r"$\overline{\mathrm{T}}_{air}$ ($\degree$C)", r"$\overline{\mathrm{SST}}$ ($\degree$C)"]]

            # Create figure and axes objects
            fig, axs = plt.subplots(nrows=3, figsize=(14, 8), constrained_layout=True)
            twins = [ax.twinx() for ax in axs]
            fig.suptitle(f"Surface Met Parameters at {ds.attrs['location_meaning']} on {date}")

            # Create the plots
            gill_data = [ds.gill_wind_speed, ds.gill_wind_direction]
            gill_labels = [r"$\overline{\mathrm{U}}$ Gill", r"$\overline{\mathrm{\theta}}$ Gill"]
            double_plot(axs[0], twins[0], data=gill_data, colors=colors, var_labels=gill_labels, linestyle="--")
            for i in range(3):
                double_plot(axs[i], twins[i], data=data[i], colors=colors, var_labels=var_labels[i], ax_labels=ax_labels[i])
                axs[i].grid(which="both", color='lightgray', linewidth=0.5)
                format_time_xticks(axs[i])
            
            # Save and close the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        # Create the second plot -- Conductivity and Sea Surface Temperature
        filename = DSUtil.get_plot_filename(dataset, "conductivity", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            data = [ds.conductivity, ds.CTD_SST]
            var_labels = [r"Conductivity (S m$^{-1}$)", r"$\overline{\mathrm{SST}}$ ($\degree$C)"]
            ax_labels = [r"Conductivity (S m$^{-1}$)", r"$\overline{\mathrm{SST}}$ ($\degree$C)"]

            fig, ax = plt.subplots(figsize=(14, 8), constrained_layout=True)

            double_plot(ax, ax.twinx(), data=data, colors=colors, var_labels=var_labels, ax_labels=ax_labels)
            ax.grid(which="both", color='lightgray', linewidth=0.5)
            format_time_xticks(ax)

            # Save and close the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        # # Custom plot for current velocity and direction
        # filename = DSUtil.get_plot_filename(dataset, "current_by_depth", "png")
        # with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
        #     fig, axes = plt.subplots(nrows=2, figsize=(15,9))
        #     dataset.current_velocity.plot(ax=axes[0], x="time", y="depth", yincrease=False, snap=True)
        #     dataset.current_direction.plot(ax=axes[1], x="time", y="depth", yincrease=False, snap=True, cmap="hsv")
        #     fig.set_tight_layout(True)
        #     fig.savefig(tmp_path)
        #     plt.close()
        #     self.storage.save(tmp_path)

        return


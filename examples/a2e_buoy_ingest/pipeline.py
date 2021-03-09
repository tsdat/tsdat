import os
import re
import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

from typing import Any, Dict, List
from tsdat.config import VariableDefinition, DatasetDefinition
from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil

example_dir = os.path.abspath(os.path.dirname(__file__))
style_file = os.path.join(example_dir, "styling.mplstyle")
plt.style.use(style_file)

class BuoyIngestPipeline(IngestPipeline):
    """-------------------------------------------------------------------
    This is an example class that extends the default IngestPipeline in
    order to hook in custom behavior such as creating custom plots.
    If users need to apply custom changes to the dataset, instrument
    corrections, or create custom plots, they should follow this example
    to extend the IngestPipeline class.
    -------------------------------------------------------------------"""
    def standardize_dataset(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        """-------------------------------------------------------------------
        Standardizes the dataset by applying variable name and units 
        conversions as defined in the config. Returns the standardized 
        dataset.

        Args:
            raw_mapping (Dict[str, xr.Dataset]):   The raw xarray dataset mapping.

        Returns:
            xr.Dataset: The standardized dataset.
        -------------------------------------------------------------------"""
        definition = self.config.dataset_definition
        
        raw_datasets = self.reduce_raw_datasets(raw_mapping, definition)

        # Merge only preserves global attributes from the first dataset.
        # Variable attributes are only preserved from the first dataset.
        merged_dataset = xr.merge(raw_datasets)

        # Check required variables are in merged dataset
        self.check_required_variables(merged_dataset, definition)

        # Ensure all variables are initialized. Computed variables,
        # variables that are set statically, and variables that weren't 
        # retrieved should be initialized
        merged_dataset = self.add_static_variables(merged_dataset, definition) 
        merged_dataset = self.maybe_initialize_variables(merged_dataset, definition)

        # Add global and variable attributes to dataset
        merged_dataset = self.add_attrs(merged_dataset, definition)

        # Add the input_files attribute to definition attributes
        merged_dataset.attrs["input_files"] = list(raw_mapping.keys())

        return merged_dataset
    
    def reduce_raw_datasets(self, raw_mapping: Dict[str, xr.Dataset], definition: DatasetDefinition) -> List[xr.Dataset]:
        """-----------------------------------------------------------------------
        For each dataset, we need to know what information to retrieve. Certainly:

            coordinates --  try to retrieve all coordinates in definition from each dataset

            data_vars   --  if variable config has file_pattern, use that to determine the
                            dataset to pull from. Otherwise, use _find_dataset_with_variable

            attributes  --  pull all variable and global attributes from each dataset.
        -----------------------------------------------------------------------"""
       
        def _find_files_with_variable(variable: VariableDefinition) -> List[xr.Dataset]:
            files = []
            variable_name = variable.get_input_name()
            for filename, dataset in raw_mapping.items():
                if variable_name in dataset.variables:
                    files.append(filename)
            return files

        def _find_files_with_regex(variable: VariableDefinition) -> List[xr.Dataset]:
            regex = re.compile(variable.input.file_pattern)
            return list(filter(regex.search, raw_mapping.keys()))

        # Determine which datasets will be used to retrieve variables
        retrieval_rules: Dict[str, List[VariableDefinition]] = {}
        for variable in definition.vars.values():
            
            if variable.has_input():
                search_func = _find_files_with_variable

                if hasattr(variable.input, "file_pattern"):
                    search_func = _find_files_with_regex

                filenames = search_func(variable)
                for filename in filenames:
                    file_rules = retrieval_rules.get(filename, [])
                    retrieval_rules[filename] = file_rules + [variable]
        
        # Build the list of reduced datasets
        reduced_datasets: List[xr.Dataset] = []
        for filename, variable_definitions in retrieval_rules.items():
            raw_dataset = raw_mapping[filename]
            reduced_dataset = self.reduce_raw_dataset(raw_dataset, variable_definitions, definition)
            reduced_datasets.append(reduced_dataset)

        return reduced_datasets

    def reduce_raw_dataset(self, raw_dataset: xr.Dataset, variable_definitions: List[VariableDefinition], definition: DatasetDefinition) -> xr.Dataset:
        """-----------------------------------------------------------------------
        For each dataset, we need to know what information to retrieve. Certainly:

            coordinates --  try to retrieve all coordinates in definition from each dataset

            data_vars   --  if variable config has file_pattern, use that to determine the
                            dataset to pull from. Otherwise, use _find_dataset_with_variable

            attributes  --  pull all variable and global attributes from each dataset.
        -----------------------------------------------------------------------"""
        def _var_retriever_and_converter(variable: VariableDefinition) -> Dict:
            # TODO: handle case where data is missing in input
            data_array = raw_dataset[variable.get_input_name()]  
            
            # Input to output unit conversion
            data = data_array.values
            in_units = variable.get_input_units()
            out_units = variable.get_output_units()
            data = variable.input.converter.run(data, in_units, out_units)

            # Consolidate retrieved data
            dictionary = {
                "attrs":    data_array.attrs,
                "dims":     list(variable.dims.keys()),
                "data":     data
            }
            return dictionary

        # Get the coordinate definitions of the given variables
        coord_names: List[str] = []
        for var_definition in variable_definitions:
            coord_names.extend(var_definition.get_coordinate_names())
        coord_names: List[str] = list(dict.fromkeys(coord_names))
        coord_definitions = [definition.get_variable(coord_name) for coord_name in coord_names]

        coords = {}
        for coordinate in coord_definitions:
            coords[coordinate.name] = _var_retriever_and_converter(coordinate)

        data_vars = {}
        for variable in variable_definitions:
            data_vars[variable.name] = _var_retriever_and_converter(variable)

        reduced_dict = {
            "attrs":        raw_dataset.attrs,
            "dims":         coord_names,
            "coords":       coords,
            "data_vars":    data_vars
        }
        return xr.Dataset.from_dict(reduced_dict)

    def check_required_variables(self, dataset: xr.Dataset, dod: DatasetDefinition):
        # TODO: Throw an error if a required variable was not retrieved in the
        # merged dataset.
        pass

    def add_static_variables(self, dataset: xr.Dataset, dod: DatasetDefinition) -> xr.Dataset:
        coords, data_vars = {}, {}
        for variable in dod.get_static_variables():
            if variable.is_coordinate():
                coords[variable.name] = variable.to_dict()
            else:
                data_vars[variable.name] = variable.to_dict()
        static_ds = xr.Dataset.from_dict({"coords": coords, "data_vars": data_vars})
        return xr.merge([dataset, static_ds])

    def maybe_initialize_variables(self, dataset: xr.Dataset, dod: DatasetDefinition):
        # Initialize variables in the dataset definition that are not in
        # the xarray dataset using the appropriate _FillValue and dimensions.
        coords, data_vars = {}, {}
        for var_name, var_def in dod.vars.items():
            if var_name not in dataset.variables:
                if var_def.is_coordinate():
                    coords[var_name] = var_def.to_dict()
                    shape = dod.dimensions[var_name].length
                    coords[var_name]["data"] = np.full(shape, var_def.get_FillValue())
                else:
                    data_vars[var_name] = var_def.to_dict()
                    shape = [len(dataset[dim_name]) for dim_name in var_def.dims.keys()]
                    data_vars[var_name]["data"] = np.full(shape, var_def.get_FillValue())

        missing_vars_ds = xr.Dataset.from_dict({"coords": coords, "data_vars": data_vars})

        return xr.merge([dataset, missing_vars_ds])

    def add_attrs(self, dataset: xr.Dataset, dod: DatasetDefinition):
        def _set_attr(obj: Any, att_name: str, att_val: Any):
            if hasattr(obj, "attrs"):
                if hasattr(obj.attrs, att_name):
                    prev_val = obj.attrs[att_name]
                    UserWarning(f"Warning: Overriding attribute {att_name}. Previously was '{prev_val}'")
                obj.attrs[att_name] = att_val
            else:
                UserWarning(f"Warning: Object {str(obj)} has no 'attrs' attribute.")

        for att in dod.attrs.values():
            _set_attr(dataset, att.name, att.value)

        for coord, coord_def in dod.coords.items():
            for att in coord_def.attrs.values():
                _set_attr(dataset[coord], att.name, att.value)

        for var, var_def in dod.vars.items():
            for att in var_def.attrs.values():
                _set_attr(dataset[var], att.name, att.value)
        
        return dataset

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
        dod = self.config.dataset_definition
        time_def = dod.get_variable("time")
        
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
            
            if "currents" in filename:
               
                def has_vel_and_dir(index: int) -> bool:
                    has_vel = f"Vel{index+1} (mm/s)" in dataset.variables
                    has_dir = f"Dir{index+1} (deg)" in dataset.variables
                    return has_vel and has_dir

                # Calculate depths and collect data vars
                i = 0
                depth, vel_data, dir_data = [], [], []
                while has_vel_and_dir(i):
                    depth.append(4 * (i + 1))
                    vel_data.append(dataset[f"Vel{i+1} (mm/s)"].data)
                    dir_data.append(dataset[f"Dir{i+1} (deg)"].data)
                    i += 1

                depth = np.array(depth)
                vel_data = np.array(vel_data).transpose()
                dir_data = np.array(dir_data).transpose()


                # vel_data  = np.array(vel_data).transpose()

                # Make time.input.name and depth coordinate variables
                dataset = dataset.set_coords(time_def.get_input_name())
                dataset["depth"] = xr.DataArray(data=depth, dims=["depth"])
                dataset = dataset.set_coords("depth")

                # Add current velocity and direction data to dataset
                dataset["current_velocity"] = xr.DataArray(data=vel_data, dims=["time", "depth"])
                dataset["current_direction"] = xr.DataArray(data=dir_data, dims=["time", "depth"])

                raw_dataset_mapping[filename] = dataset

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
        # No customizations to perform; return original dataset
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

        def add_colorbar(ax, plot, label):
            cb = plt.colorbar(plot, ax=ax, pad=0.01)
            cb.ax.set_ylabel(label, fontsize=12)
            cb.outline.set_linewidth(1)
            cb.ax.tick_params(size=0)
            cb.ax.minorticks_off()
            return cb
        
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
            fig.suptitle(f"Conductivity and Sea Surface Temperature at {ds.attrs['location_meaning']} on {date}")

            double_plot(ax, ax.twinx(), data=data, colors=colors, var_labels=var_labels, ax_labels=ax_labels)
            ax.grid(which="both", color='lightgray', linewidth=0.5)
            format_time_xticks(ax)

            # Save and close the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        # Create third plot -- current direction and velocity
        filename = DSUtil.get_plot_filename(dataset, "currents", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            ds: xr.Dataset = dataset.resample(time="1H").mean(keep_attrs=True)
            ds: xr.Dataset = ds.coarsen(depth=2, keep_attrs=True).mean(keep_attrs=True)

            # Calculations for contour plots
            date = pd.to_datetime(ds.time.data[0]).strftime('%d-%b-%Y')
            hi = np.ceil(ds.current_velocity.max().data + 1)
            lo = np.floor(ds.current_velocity.min().data)
            levels = 30

            # Calculations for quiver plot
            qv_degrees = ds.current_direction.data.transpose()
            qv_theta = (qv_degrees + 90) * (np.pi/180)
            X, Y = ds.time.data, ds.depth.data
            U, V = np.cos(-qv_theta), np.sin(-qv_theta)

            # Create figure and axes objects
            fig, ax = plt.subplots(figsize=(16,8), constrained_layout=True)
            fig.suptitle(f"Average current speed and direction at {ds.attrs['location_meaning']} on {date}")

            # Make top subplot -- contours and quiver plots for wind speed and direction
            csf = ds.current_velocity.plot.contourf(ax=ax, x="time", yincrease=False, levels=levels, cmap=cmocean.cm.deep_r, add_colorbar=False)
            ds.current_velocity.plot.contour(ax=ax, x="time", yincrease=False, levels=levels, colors="lightgray", linewidths=0.5)
            ax.quiver(X, Y, U, V, width=0.002, scale=60, color="white", zorder=10, label="Current Direction (degrees)")
            cb = add_colorbar(ax, csf, r"Current Speed (mm s$^{-1}$)")
            format_time_xticks(ax)
            ax.set_ylabel("Depth (m)")

            # Save the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()
        return

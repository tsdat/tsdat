import os
import cmocean
import numpy as np
import xarray as xr
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

from typing import Dict
from tsdat.io import FileHandler
from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil

example_dir = os.path.abspath(os.path.dirname(__file__))
style_file = os.path.join(example_dir, "styling.mplstyle")
plt.style.use(style_file)


class StaPipeline(IngestPipeline):
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
            if ".sta" in raw_filename:

                raw_categories = ["Wind Speed (m/s)", "Wind Direction (�)", "Data Availability (%)"]
                output_var_names = ["wind_speed", "wind_direction", "data_availability"]
                heights = dataset.height.data
                for category, output_name in zip(raw_categories, output_var_names):
                    var_names = [f"{height}m {category}" for height in heights]
                    var_data  = [raw_dataset[name].data for name in var_names]
                    var_data  = np.array(var_data).transpose()
                    dataset[output_name].data = var_data

                # num_bins = 50
                # # Want to add Vel1 (mm/s), Vel2 (mm/s), Vel3 (mm/s), ... to the 
                # # current_velocity variable so that it is two dimensional.
                # vel_names = [f"Vel{i} (mm/s)" for i in range(1, num_bins + 1)]
                # vel_data  = [raw_dataset[name].data for name in vel_names]
                # vel_data  = np.array(vel_data).transpose()
                # dataset["current_velocity"].data = vel_data

                # # Want to add Dir1 (deg), Dir2 (deg), Dir3 (deg), ... to the 
                # # current_direction variable so that it is two dimensional.
                # vel_names = [f"Dir{i} (deg)" for i in range(1, num_bins + 1)]
                # vel_data  = [raw_dataset[name].data for name in vel_names]
                # vel_data  = np.array(vel_data).transpose()
                # dataset["current_direction"].data = vel_data
        
        return dataset
    
    def create_and_persist_plots(self, dataset: xr.Dataset):

        def format_time_xticks(ax, time_da: xr.DataArray, start=6*4-1, step=6*4, date_format="%H-%M"):
            """
            Sets the xticks, xtick labels, and xaxis major tick formatting based on given parameters.
            By default, assumes that data is on 10-minute time scale and sets major ticks every 4 hours
            across the day.
            """
            # _ = ax.set_xticks(time_da.data[np.arange(start, len(time_da)-1, step)], minor=True)
            _ = ax.set_xticks(time_da.data[np.arange(start, len(time_da)-1, step)], minor=False)
            _ = ax.set_xticklabels(ax.get_xticks(), rotation=0)
            _ = ax.xaxis.set_major_formatter(mpl.dates.DateFormatter(date_format))
            return
        
        def add_colorbar(ax, plot, label):
            cb = plt.colorbar(plot, ax=ax, pad=0.01)
            cb.ax.set_ylabel(label, fontsize=12)
            cb.outline.set_linewidth(1)
            cb.ax.tick_params(size=0)
            cb.ax.minorticks_off()
            return cb

        ds = dataset

        
        filename = DSUtil.get_plot_filename(dataset, "wind_speed_and_direction", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            
            # Calculations for contour plots
            date = pd.to_datetime(ds.time.data[0]).strftime('%d-%b-%Y')
            hi = np.ceil(ds.wind_speed.max().data + 1)
            lo = np.floor(ds.wind_speed.min().data)
            levels = np.arange(lo, hi, 1)

            # Calculations for quiver plot
            qv_spacing = int(np.ceil(len(ds.time) / 24))
            qv_degrees = ds.wind_direction.data[::qv_spacing].transpose()
            qv_theta = (qv_degrees + 90) * (np.pi/180)
            X, Y = ds.time.data[::qv_spacing], ds.height.data
            U, V = np.cos(-qv_theta), np.sin(-qv_theta)

            # Colormaps to use
            wind_cmap = cmocean.cm.deep_r
            avail_cmap = cmocean.cm.amp_r

            # Create figure and axes objects
            fig, axs = plt.subplots(nrows=2, figsize=(16,8), constrained_layout=True)
            fig.suptitle(f"Average wind speed and direction at {ds.attrs['location_meaning']} on {date}")

            # Make top subplot -- contours and quiver plots for wind speed and direction
            csf = ds.wind_speed.plot.contourf(ax=axs[0], x="time", levels=levels, cmap=wind_cmap, add_colorbar=False)
            ds.wind_speed.plot.contour(ax=axs[0], x="time", levels=levels, colors="lightgray", linewidths=0.5)
            axs[0].quiver(X, Y, U, V, width=0.002, scale=60, color="white", zorder=10, label="Wind Direction (degrees)")
            cb = add_colorbar(axs[0], csf, r"Wind Speed (ms$^{-1}$)")
            format_time_xticks(axs[0], ds.time)
            axs[0].set_ylabel("Height ASL (m)")

            # Make bottom subplot -- heatmap for data availability
            da = ds.data_availability.plot(ax=axs[1], x="time", cmap=avail_cmap, add_colorbar=False)
            add_colorbar(axs[1], da, "Availability (%)")
            format_time_xticks(axs[1], ds.time)
            axs[1].set_ylabel("Height ASL (m)")

            # Save the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        # Figure 2. -- Lidar Wind Speeds at several elevations
        filename = DSUtil.get_plot_filename(dataset, "wind_speed_slices", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

            # Select heights to plot
            heights = [40, 90, 140, 200]

            # Create the figure and axes objects
            fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(12,6), constrained_layout=True)
            
            # Plot the data
            for i, height in enumerate(heights):
                velocity = ds.wind_speed.sel(height=height)
                velocity.plot(ax=ax, linewidth=2, c=wind_cmap(i/len(heights)), label=f"{height} m")
            
            # Add useful information and do some cleaning
            format_time_xticks(ax, ds.time)
            ax.legend(loc="best", facecolor="white", ncol=len(heights))
            ax.set_title("") # Remove bogus title created by xarray
            ax.set_xlabel("Time (UTC)")
            ax.set_ylabel(r"Wind Speed (ms$^{-1}$)")

            # Save the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        return


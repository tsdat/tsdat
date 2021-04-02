import os
import cmocean
import numpy as np
import xarray as xr
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

from typing import Dict
from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil

example_dir = os.path.abspath(os.path.dirname(__file__))
style_file = os.path.join(example_dir, "styling.mplstyle")
plt.style.use(style_file)


class StaPipeline(IngestPipeline):

    def apply_corrections(self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        if "morro" in dataset.attrs["datastream"]:
            dataset["wind_direction"].data = (dataset["wind_direction"].data + 180) % 360
            dataset["wind_direction"].attrs["corrections_applied"] = "Applied +180 degree calibration factor."
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

        def format_time_xticks(ax, /, *, start=4, stop=21, step=4, date_format="%H-%M"):
            ax.xaxis.set_major_locator(mpl.dates.HourLocator(byhour=range(start, stop, step)))
            ax.xaxis.set_major_formatter(mpl.dates.DateFormatter(date_format))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center')
        
        def add_colorbar(ax, plot, label):
            cb = plt.colorbar(plot, ax=ax, pad=0.01)
            cb.ax.set_ylabel(label, fontsize=12)
            cb.outline.set_linewidth(1)
            cb.ax.tick_params(size=0)
            cb.ax.minorticks_off()
            return cb

        ds = dataset
        date = pd.to_datetime(ds.time.data[0]).strftime('%d-%b-%Y')
        
        # Colormaps to use
        wind_cmap = cmocean.cm.deep_r
        avail_cmap = cmocean.cm.amp_r

        # Create the first plot - Lidar Wind Speeds at several elevations
        filename = DSUtil.get_plot_filename(dataset, "wind_speed_slices", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

            # Create the figure and axes objects
            fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(14,8), constrained_layout=True)
            fig.suptitle(f"Wind Speed Slices at {ds.attrs['location_meaning']} on {date}")

            # Select heights to plot
            heights = [40, 90, 140, 200]
            
            # Plot the data
            for i, height in enumerate(heights):
                velocity = ds.wind_speed.sel(height=height)
                velocity.plot(ax=ax, linewidth=2, c=wind_cmap(i/len(heights)), label=f"{height} m")
            
            # Set the labels and ticks
            format_time_xticks(ax)
            ax.legend(facecolor="white", ncol=len(heights), bbox_to_anchor=(1, -0.05))
            ax.set_title("") # Remove bogus title created by xarray
            ax.set_xlabel("Time (UTC)")
            ax.set_ylabel(r"Wind Speed (ms$^{-1}$)")

            # Save the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()
        
        filename = DSUtil.get_plot_filename(dataset, "wind_speed_and_direction", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
            
            # Reduce dimensionality of dataset for plotting
            ds: xr.Dataset = ds.resample(time="1H").nearest()

            # Calculations for contour plots
            levels = 30

            # Calculations for quiver plot
            qv_slice = slice(1, -1)  # Skip first and last to prevent weird overlap with axes borders
            qv_degrees = ds.wind_direction.data[qv_slice].transpose()
            qv_theta = (qv_degrees + 90) * (np.pi/180)
            X, Y = ds.time.data[qv_slice], ds.height.data
            U, V = np.cos(-qv_theta), np.sin(-qv_theta)

            # Create figure and axes objects
            fig, axs = plt.subplots(nrows=2, figsize=(14,8), constrained_layout=True)
            fig.suptitle(f"Wind Speed and Direction at {ds.attrs['location_meaning']} on {date}")

            # Make top subplot -- contours and quiver plots for wind speed and direction
            csf = ds.wind_speed.plot.contourf(ax=axs[0], x="time", levels=levels, cmap=wind_cmap, add_colorbar=False)
            ds.wind_speed.plot.contour(ax=axs[0], x="time", levels=levels, colors="lightgray", linewidths=0.5)
            axs[0].quiver(X, Y, U, V, width=0.002, scale=60, color="white", pivot='middle', zorder=10)
            add_colorbar(axs[0], csf, r"Wind Speed (ms$^{-1}$)")

            # Make bottom subplot -- heatmap for data availability
            da = ds.data_availability.plot(ax=axs[1], x="time", cmap=avail_cmap, add_colorbar=False)
            add_colorbar(axs[1], da, "Availability (%)")

            # Set the labels and ticks
            for i in range(2):
                format_time_xticks(axs[i])
                axs[i].set_xlabel("Time (UTC)")
                axs[i].set_ylabel("Height ASL (m)")

            # Save the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()


        return


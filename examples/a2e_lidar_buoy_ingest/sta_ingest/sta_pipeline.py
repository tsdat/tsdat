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
        
        def format_colorbar(cbar, ylabel=""):
            _ = cb.ax.set_ylabel(ylabel)
            _ = cb.outline.set_linewidth(1)# Reduce outline width
            _ = cb.ax.tick_params(size=0)  # Remove major ticks
            _ = cb.ax.minorticks_off()     # Remove minor ticks

        ds = dataset

        # Styling parameters:
        mpl.rcParams['font.family']         = 'serif'
        mpl.rcParams['font.size']           = 12
        mpl.rcParams['legend.frameon']      = True
        mpl.rcParams['legend.framealpha']   = 1
        mpl.rcParams['legend.edgecolor']    = 'w'
        mpl.rcParams['axes.linewidth']      = 2

        mpl.rcParams['xtick.top']           = True
        mpl.rcParams['xtick.minor.visible'] = True
        mpl.rcParams['xtick.minor.size']    = 5
        mpl.rcParams['xtick.major.size']    = 8
        mpl.rcParams['xtick.minor.width']   = 2
        mpl.rcParams['xtick.major.width']   = 2

        mpl.rcParams['ytick.right']         = True
        mpl.rcParams['ytick.minor.visible'] = True
        mpl.rcParams['ytick.minor.size']    = 5
        mpl.rcParams['ytick.major.size']    = 8
        mpl.rcParams['ytick.minor.width']   = 2
        mpl.rcParams['ytick.major.width']   = 2

        
        filename = DSUtil.get_plot_filename(dataset, "wind_speed_and_direction", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

            # Extract the date
            date = pd.to_datetime(ds.time.data[0])

            # Spacing every 1 m/s for contours
            hi = np.ceil(ds.wind_speed.max().data + 1)
            lo = np.floor(ds.wind_speed.min().data)
            levels = np.arange(lo, hi, 1)

            # Create figure and set title
            fig, axs = plt.subplots(ncols=1, nrows=2, figsize=(16,8), constrained_layout=True)
            _ = fig.suptitle(f"Average wind speed and direction at {ds.attrs['location_meaning']} on {date.strftime('%d-%b-%Y')}")

            # Contour Plot + Contour lines
            csf = ds.wind_speed.plot.contourf(x="time", y="height", ax=axs[0], levels=levels, cmap=cmocean.cm.deep_r, add_colorbar=False)
            cs  = ds.wind_speed.plot.contour(x="time", y="height", ax=axs[0], levels=levels, colors="lightgray", linewidths=0.5)
            cb  = plt.colorbar(csf, ax=axs[0], pad=0.01)
            format_colorbar(cb, ylabel=r"Wind Speed (ms$^{-1}$)")
            format_time_xticks(axs[0], ds.time)
            _   = axs[0].set_xlabel("Time (UTC)")
            _   = axs[0].set_ylabel("Height ASL (m)")

            # Quiver plot
            spacing = int(round(len(ds.wind_speed) / 24))
            x, y    = np.meshgrid(ds.time.data[::spacing], np.linspace(40, int(ds.height.max().data), len(ds.height.data)))
            r       = ds.wind_speed.data[::spacing]
            o       = (ds.wind_direction.data[::spacing] + 90) * (np.pi/180)
            u, v    = np.cos(-o), np.sin(-o)
            qv      = axs[0].quiver(x, y, u, v, width=0.002, scale=60, color="white", zorder=10, label="Wind Direction (degrees)")
            lgnd    = axs[0].legend(loc="best", facecolor="lightgray")
            _       = lgnd.set_zorder(11)

            # Data availability plot
            da = ds.data_availability.plot(ax=axs[1], x="time", y="height", cmap=cmocean.cm.amp_r, add_colorbar=False)
            cb = plt.colorbar(da, ax=axs[1], pad=0.01)
            format_colorbar(cb, ylabel="Availability (%)")
            format_time_xticks(axs[1], ds.time)
            _  = axs[1].set_xlabel("Time (UTC)")
            _  = axs[1].set_ylabel("Height ASL (m)")

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
                _ = velocity.plot(ax=ax, linewidth=2, c=cmocean.cm.deep_r(i/len(heights)), label=f"{height} m")
            
            # Add useful information and do some cleaning
            format_time_xticks(ax, ds.time)
            _ = ax.legend(loc="best", facecolor="white", ncol=len(heights))
            _ = ax.set_title("") # Remove bogus title created by xarray
            _ = ax.set_xlabel("Time (UTC)")
            _ = ax.set_ylabel(r"Wind Speed (ms$^{-1}$)")

            # Save the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()


        return


import os
import pandas as pd
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
from cmocean.cm import amp_r, dense, haline

from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil

example_dir = os.path.abspath(os.path.dirname(__file__))
style_file = os.path.join(example_dir, "styling.mplstyle")
plt.style.use(style_file)


class WaveIngestPipeline(IngestPipeline):
    """-------------------------------------------------------------------
    This is an example class that extends the default IngestPipeline in
    order to hook in custom behavior such as creating custom plots.
    If users need to apply custom changes to the dataset, instrument
    corrections, or create custom plots, they should follow this example
    to extend the IngestPipeline class.
    -------------------------------------------------------------------"""

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset) -> None:
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
            fig.savefig(tmp_path)
            self.storage.save(tmp_path)
            plt.close()

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

        def format_time_xticks(ax, start=4, stop=21, step=4, date_format="%H-%M"):
            ax.xaxis.set_major_locator(
                mpl.dates.HourLocator(byhour=range(start, stop, step))
            )
            ax.xaxis.set_major_formatter(mpl.dates.DateFormatter(date_format))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

        # Useful variables
        ds = dataset
        date = pd.to_datetime(ds.time.data[0]).strftime("%d-%b-%Y")

        # Create wave statistics plot
        filename = DSUtil.get_plot_filename(dataset, "wave_statistics", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

            # Create figure and axes objects
            fig, axs = plt.subplots(nrows=3, figsize=(14, 8), constrained_layout=True)
            fig.suptitle(f"Wave Statistics at {ds.attrs['location_meaning']} on {date}")

            # Plot Wave Heights
            c1, c2, c3 = amp_r(0.10), amp_r(0.50), amp_r(0.85)
            ds.mean_wave_height.plot(ax=axs[0], c=c1, label=r"H$_{mean}$")
            ds.significant_wave_height.plot(ax=axs[0], c=c2, label=r"H$_{sig}$")
            ds.max_wave_height.plot(ax=axs[0], c=c3, label=r"H$_{max}$")
            axs[0].legend(bbox_to_anchor=(1, -0.10), ncol=3)
            axs[0].set_ylabel("Wave Height (m)")

            # Plot Wave Periods
            c1, c2, c3 = dense(0.15), dense(0.50), dense(0.8)
            ds.mean_wave_period.plot(ax=axs[1], c=c1, label=r"T$_{mean}$")
            ds.peak_wave_period.plot(ax=axs[1], c=c2, label=r"T$_{peak}$")
            ds.max_wave_period.plot(ax=axs[1], c=c3, label=r"T$_{max}$")
            axs[1].legend(bbox_to_anchor=(1, -0.10), ncol=3)
            axs[1].set_ylabel("Wave Period (s)")

            # Plot Wave Directions
            c1, c2 = haline(0.15), haline(0.4)
            ds.mean_wave_direction.plot(ax=axs[2], c=c1, label=r"$\theta_{mean}$")
            ds.peak_wave_direction.plot(ax=axs[2], c=c2, label=r"$\theta_{peak}$")
            axs[2].legend(bbox_to_anchor=(1, -0.10))
            axs[2].set_ylabel("Wave Direction (deg)")

            for i in range(3):
                axs[i].set_xlabel("Time (UTC)")
                format_time_xticks(axs[i])

            # Save figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        return

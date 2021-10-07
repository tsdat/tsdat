import os
import cmocean
import pandas as pd
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt

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

            # Plot wave heights
            cmap = cmocean.cm.amp_r
            ds.average_wave_height.plot(
                ax=axs[0], c=cmap(0.10), linewidth=2, label=r"H$_{avg}$"
            )
            ds.significant_wave_height.plot(
                ax=axs[0], c=cmap(0.5), linewidth=2, label=r"H$_{sig}$"
            )
            ds.max_wave_height.plot(
                ax=axs[0], c=cmap(0.85), linewidth=2, label=r"H$_{max}$"
            )
            axs[0].set_ylabel("Wave Height (m)")
            axs[0].legend(bbox_to_anchor=(1, -0.10), ncol=3)

            # Plot wave periods
            cmap = cmocean.cm.dense
            ds.average_wave_period.plot(
                ax=axs[1], c=cmap(0.15), linewidth=2, label=r"T$_{avg}$"
            )
            ds.significant_wave_period.plot(
                ax=axs[1], c=cmap(0.5), linewidth=2, label=r"T$_{sig}$"
            )
            ds.mean_wave_period.plot(
                ax=axs[1], c=cmap(0.8), linewidth=2, label=r"$\overline{T}_{mean}$"
            )
            axs[1].set_ylabel("Wave Period (s)")
            axs[1].legend(bbox_to_anchor=(1, -0.10), ncol=3)

            # Plot mean direction
            cmap = cmocean.cm.haline
            ds.mean_wave_direction.plot(
                ax=axs[2], c=cmap(0.4), linewidth=2, label=r"$\overline{\phi}_{mean}$"
            )
            axs[2].set_ylabel(r"Wave $\overline{\phi}$ (deg)")
            axs[2].legend(bbox_to_anchor=(1, -0.10))

            # Set xlabels and ticks
            for i in range(3):
                axs[i].set_xlabel("Time (UTC)")
                format_time_xticks(axs[i])

            # Save figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        return

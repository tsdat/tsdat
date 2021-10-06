import matplotlib.pyplot as plt
import os
import pandas as pd
import xarray as xr

from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil

example_dir = os.path.abspath(os.path.dirname(__file__))
style_file = os.path.join(example_dir, "styling.mplstyle")
plt.style.use(style_file)


class ImuIngestPipeline(IngestPipeline):
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
        ds = dataset

        # Useful values
        location = ds.attrs["location_meaning"]
        date1, date2 = pd.to_datetime(ds.time.data[0]), pd.to_datetime(ds.time.data[-1])
        hhmm1, hhmm2 = date1.strftime("%H:%M"), date2.strftime("%H:%M")
        date = date1.strftime("%d-%b-%Y")

        filename = DSUtil.get_plot_filename(dataset, "buoy_motion_histogram", "png")
        with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

            fig, ax = plt.subplots(figsize=(14, 8), constrained_layout=True)

            # Create plot labels including mean roll/pitch
            mean_roll, mean_pitch = ds["roll"].mean().data, ds["pitch"].mean().data
            roll_label = (
                r"$\.{\theta}_{roll}$ [$\overline{\theta}_r$ ="
                + f"{mean_roll:.3f} deg]"
            )
            pitch_label = (
                r"$\.{\theta}_{pitch}$ [$\overline{\theta}_p$ ="
                + f"{mean_pitch:.3f} deg]"
            )

            # Plot the stepped
            ds["roll"].plot.hist(
                ax=ax, linewidth=2, edgecolor="black", histtype="step", label=roll_label
            )
            ds["pitch"].plot.hist(
                ax=ax, linewidth=2, edgecolor="red", histtype="step", label=pitch_label
            )

            # Set axes and figure labels
            fig.suptitle(
                f"Buoy Motion Histogram at {location} on {date} from {hhmm1} to {hhmm2}"
            )
            ax.set_xlabel("Buoy Motion (deg)")
            ax.set_ylabel("Frequency")
            ax.set_title("")
            ax.legend(ncol=2, bbox_to_anchor=(1, -0.04))

            # Save the figure
            fig.savefig(tmp_path, dpi=100)
            self.storage.save(tmp_path)
            plt.close()

        return

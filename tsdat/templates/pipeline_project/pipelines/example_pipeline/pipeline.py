import act
import matplotlib.pyplot as plt
import xarray as xr
from cmocean.cm import amp_r, dense, haline
from tsdat import IngestPipeline, get_start_date_and_time_str

from utils import format_time_xticks


class ExamplePipeline(IngestPipeline):
    """---------------------------------------------------------------------------------
    This is an example ingestion pipeline meant to demonstrate how one might set up a
    pipeline using this template repository.

    ---------------------------------------------------------------------------------"""

    def hook_plot_dataset(self, dataset: xr.Dataset):
        ds = dataset
        loc = self.dataset_config.attrs.location_id

        date, time = get_start_date_and_time_str(dataset)

        with plt.style.context("shared/styling.mplstyle"):
            fig, axs = plt.subplots(nrows=3)
            fig.suptitle(f"Wave Statistics at {loc} on {date} {time}")

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
            axs[2].legend(bbox_to_anchor=(1, -0.10), ncol=2)
            axs[2].set_ylabel("Wave Direction (deg)")

            for i in range(3):
                axs[i].set_xlabel("Time (UTC)")
                format_time_xticks(axs[i])

            plot_file = self.get_ancillary_filepath(title="wave_data_plots")
            fig.savefig(plot_file)  # type: ignore
            plt.close(fig)

            # Create Plot Display
            variable = "significant_wave_height"
            display = act.plotting.TimeSeriesDisplay(
                ds, figsize=(15, 10), subplot_shape=(2,)
            )
            # Plot data in top plot
            display.plot(variable, subplot_index=(0,), label="Wave Height")
            # Plot QC data
            display.qc_flag_block_plot(variable, subplot_index=(1,))
            fig = display.fig

            plot_file = self.get_ancillary_filepath(title="wave_height")
            fig.savefig(plot_file)  # type: ignore
            plt.close(fig)

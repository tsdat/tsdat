import xarray as xr
from matplotlib import pyplot as plt

import tsdat


class Ingest(tsdat.IngestPipeline):
    def hook_plot_dataset(self, dataset: xr.Dataset):
        fig, ax = plt.subplots()
        ax.plot([1, 2], [3, 4])
        filepath = self.get_ancillary_filepath("example")
        fig.savefig(filepath)

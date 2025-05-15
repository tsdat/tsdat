from typing import TYPE_CHECKING, Any, Optional

import xarray as xr

from ...io.base import DataConverter, RetrievedDataset
from ..bin_average.calculate_bin_average import calculate_bin_average
from ..utils.create_bounds import create_bounds_from_labels
from ..utils.is_metric_var import is_metric_var
from ..utils.is_qc_var import is_qc_var

# Prevent any chance of runtime circular imports for typing-only imports
if TYPE_CHECKING:  # pragma: no cover
    from ...config.dataset import DatasetConfig  # pragma: no cover
    from ...io.retrievers import StorageRetriever  # pragma: no cover


class BinAverage(DataConverter):
    """Saves data into the specified coordinate grid using bin-averaging."""

    # TODO: Implement "ALL" -- default to iterating over all the variable's coords
    coord: str = "time"
    """The coordinate axis this converter should be applied on. Defaults to 'time'."""

    keep_metrics: bool = True
    """If true, then transform metrics will be preserved in the output dataset as new
    data variables ('<variable_name>_std' and '<variable_name>_goodfraction')."""

    keep_qc: bool = True
    """If true, then transform qc checks will be preserved in the output dataset as a
    new data variable ('qc_<variable_name>')."""

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: "DatasetConfig",
        retrieved_dataset: RetrievedDataset,
        retriever: Optional["StorageRetriever"] = None,
        input_dataset: Optional[xr.Dataset] = None,
        input_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        # ############################################################################ #
        # Perform sanity checks on the arguments provided. There should probably be a
        # better way to do these things than in each converter, but I don't have time to
        # figure that out now.

        # Coordinate variables can't be the subject of a bin average transformation. The
        # user probably made a mistake in the config file.
        if variable_name in dataset_config.coords:
            raise ValueError(
                f"{self.__repr_name__} cannot be used for coordinate variables."
                f" Offending coord: '{variable_name}'."
            )

        # The variable being transformed has to have the coordinate in its dimensions.
        # If it doesn't, then we can do nothing here so we just skip.
        if self.coord not in dataset_config[variable_name].dims:
            return None

        # Some variables can't be transformed here - specifically bounds variables, qc
        # variables, and transformation metrics variables.
        if variable_name.endswith("_bounds") or is_qc_var(data) or is_metric_var(data):
            return None

        assert retriever is not None
        assert retriever.parameters is not None
        assert retriever.parameters.trans_params is not None
        assert input_dataset is not None
        assert input_key is not None

        # ############################################################################ #
        # Get parameters and start building up the pieces we need to perform the bin
        # averaging transformation.

        # First, the input pieces we need: the input variable name, input coord name(s),
        # and other inputs we need as well. Note that we need this info as a mapping
        # because later on in the pipeline things are renamed, so we need to undo whatever
        retrieved_var = retriever.match_data_var(variable_name, input_key)
        retrieved_coord = retriever.match_coord(self.coord, input_key)
        assert retrieved_var is not None
        assert retrieved_coord is not None
        # TODO: Need to assert the coord is not NA (something we used to do)

        # Limit the input dataset to just the selected variable (and its ancillary qc
        # and metrics variables, if present). Also include the coordinate bounds if
        # available as well.
        input_var_name = retrieved_var.name  # TODO: this is a str | list[str] type. We
        input_coord_name = retrieved_coord.name  # TODO: need to get the actual name.
        input_bounds = f"{input_coord_name}_bounds"
        input_qc_name = f"qc_{input_var_name}"
        input_std_name = f"{input_var_name}_std"
        input_goodfrac_name = f"{input_var_name}_goodfraction"

        input_var_list = [
            input_bounds,
            input_var_name,
            input_qc_name,
            input_std_name,
            input_goodfrac_name,
        ]
        input_var_list = [v for v in input_var_list if v in input_dataset]
        filtered_input_dataset = input_dataset[input_var_list]

        # Get the corresponding output variable names and rename things in our copy of
        # the input dataset to match the output dataset structure
        output_bounds = f"{self.coord}_bounds"
        output_qc_name = f"qc_{variable_name}"
        output_std_name = f"{variable_name}_std"
        output_goodfrac_name = f"{variable_name}_goodfraction"

        variable_mapping = {
            input_var_name: variable_name,
            input_coord_name: self.coord,
            input_bounds: output_bounds,
            input_qc_name: output_qc_name,
            input_std_name: output_std_name,
            input_goodfrac_name: output_goodfrac_name,
        }
        variable_mapping = {
            k: v for k, v in variable_mapping.items() if k in input_dataset
        }
        filtered_input_dataset = filtered_input_dataset.rename(variable_mapping)

        # Get the transformation parameters. This is a dictionary that has 3 keys:
        # 'alignment', 'range', and 'width'. For each entry of those there is another
        # dictionary for each coordinate to transform with the corresponding value,
        # which will be either a string or a number.
        trans_params = retriever.parameters.trans_params.select_parameters(input_key)
        t_align = trans_params["alignment"][self.coord]
        t_width = trans_params["width"][self.coord]
        assert t_align is not None
        assert t_width is not None

        # Get the output coordinate labels and generate the 'bounds' corresponding with
        # those labels. I say 'bounds' in quotes because these are really more related
        # to the transform logic than they are necessarily to the real coord bounds, as
        # described in the next comment block below.
        labels = retrieved_dataset.coords[self.coord].values
        bounds = create_bounds_from_labels(
            labels=labels,
            width=t_width,
            alignment=t_align.lower(),
        )

        # Create the bounds used for the transform. Note that this may be different from
        # the actual coordinate bounds in the output dataset. When I wrote the code for
        # the bin average transform, I intended for the bounds to be used to select
        # which input points are used for each output point, allowing for overlap. I
        # think this is probably what should be used for the output bounds too, so there
        # is some trace of what happened here in the output dataset, **BUT**, there is a
        # problem. Sometimes you want to handle different bin widths for different
        # variables in the same dataset -- how do you deal with the coordinate bounds in
        # that case? I don't see a good solution, so we just leave it as is – these
        # bounds are temporary.
        # labels, bounds = create_bounds(
        #     start=...,
        #     stop=...,
        #     interval=...,
        #     width=t_width,
        #     alignment=t_align.lower(),
        # )

        # ############################################################################ #
        # Do the actual transformation and extract the information we want to keep from
        # the resulting xarray dataset

        # Do the transformation
        avg_ds = calculate_bin_average(
            input_dataset=filtered_input_dataset,
            coord_name=self.coord,
            coord_labels=labels,
            coord_bounds=bounds,
        )
        # TODO: Input variables need to be renamed to match the correct outputs, especially

        # The output dataset dictionary. Assigning or updating values in this dictionary
        # persists in the results outside of this DataConverter. The keys/variable names
        # used here are the same as those in the output dataset.
        output = retrieved_dataset.data_vars

        # Assign the averaged variable to the output dataset structure
        output[variable_name] = avg_ds[variable_name]

        # Only assign the qc variable to the output dataset structure if requested.
        if self.keep_qc:
            output[output_qc_name] = avg_ds[output_qc_name]

        # Only assign metrics variables to the output dataset structure if requested.
        if self.keep_metrics:
            output[output_std_name] = avg_ds[output_std_name]
            output[output_goodfrac_name] = avg_ds[output_goodfrac_name]

        # Because we are updating the output dataset structure manually, we don't need
        # (or want) to return an xarray DataArray. If we did, then the code that runs
        # through the DataConverters would assign the xr.DataArray to the output
        # structure anyways, which is probably fine, but unnecessary.
        return None

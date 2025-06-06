from typing import TYPE_CHECKING, Any, Optional

import xarray as xr

from ...io.base import DataConverter, RetrievedDataset
from ..bin_average.calculate_bin_average import calculate_bin_average
from ..utils.create_bounds import create_bounds_from_labels
from ..utils.is_metric_var import is_metric_var
from ..utils.is_qc_var import is_qc_var
from ..utils.replace_qc_attr import replace_qc_attr

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

        # Get the list of associated variable names (qc, bounds, metrics). These
        # correspond with entries in the output structure and the retrieved_dataset
        # object (if the user has retrieved those variables).
        output_bounds_name = f"{self.coord}_bounds"
        output_qc_name = f"qc_{variable_name}"
        output_std_name = f"{variable_name}_std"
        output_goodfrac_name = f"{variable_name}_goodfraction"

        # Like above, but for the input dataset. We preferentially pull from the
        # retrieved dataset, but if the variable exists in the input we just assume that
        # the user forgot about it, and pull it in here.
        # TODO: is this a good idea? without it, a bunch of qc variables will not be
        # retrieved, but with it there is no way to say you don't want to retrieve those.
        input_var_name = retrieved_var.name
        input_coord_name = retrieved_coord.name
        # TODO: input_var and input_coord types are str | list[str] type. want str only.
        input_bounds = f"{input_coord_name}_bounds"
        input_qc_name = f"qc_{input_var_name}"
        input_std_name = f"{input_var_name}_std"
        input_goodfrac_name = f"{input_var_name}_goodfraction"

        input_to_output_variables = {
            input_bounds: output_bounds_name,
            input_qc_name: output_qc_name,
            input_std_name: output_std_name,
            input_goodfrac_name: output_goodfrac_name,
        }

        # The above approach works when there is only 1 transformation converter applied
        # but it breaks if there are multiple applied in series. This is because the
        # results of the transformation are saved to the *retrieved* dataset dictionary,
        # *not* the input_dataset. To address this, we start with the provided DataArray
        # from the retrieved dataset and add onto it the ancillary variables from the
        # retrieved dataset dictionary, so long as the shapes match. We also ensure that
        # this xr.Dataset has the variable names of the output dataset.

        # Build the actual input dataset for the transformation. We primarily pull from
        # the retrieved dataset, but can also pull from the input_dataset if a variable
        # is missing that should have been retrieved.
        # TODO: Maybe there should be a warning? Should the retriever be updated to add
        # QC retrievals and these other things? Pulling from input_dataset should be a
        # last-resort.
        dataset = data.to_dataset(name=variable_name)
        for input_var_name, output_var_name in input_to_output_variables.items():
            if output_var_name in retrieved_dataset.data_vars:
                ret_data = retrieved_dataset.data_vars[output_var_name]
                dataset[output_var_name] = ret_data
            elif input_var_name in input_dataset.data_vars:
                in_data = input_dataset[input_var_name]
                dataset[output_var_name] = in_data
            else:
                # The ancillary data simply does not exist, which is fine.
                continue
        try:
            # Try to ensure the coordinate is named as it is in the output dataset. This
            # is not always the case since 'data' can still have the original coordinate
            # name and dimensions. Xarray raises a ValueError if the input name isn't in
            # the dataset, which probably means that we already have the correct name.
            dataset = dataset.rename({input_coord_name: self.coord})
        except ValueError:
            coord_index = dataset_config[variable_name].dims.index(self.coord)
            current_coord_name = tuple(data.coords.keys())[coord_index]
            dataset = dataset.rename({current_coord_name: self.coord})

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

        # ############################################################################ #
        # Do the actual transformation and extract the information we want to keep from
        # the resulting xarray dataset

        # Do the transformation
        avg_ds = calculate_bin_average(
            input_dataset=dataset,
            coord_name=self.coord,
            coord_labels=labels,
            coord_bounds=bounds,
        )

        # The output dataset dictionary. Assigning or updating values in this dictionary
        # persists in the results outside of this DataConverter. The keys/variable names
        # used here are the same as those in the output dataset.
        output = retrieved_dataset.data_vars

        # Assign the averaged variable to the output dataset structure
        output[variable_name] = avg_ds[variable_name]

        # Only assign the qc variable to the output dataset structure if requested.
        if self.keep_qc:
            output[output_qc_name] = avg_ds[output_qc_name]
            output = replace_qc_attr(
                output, retrieved_var.name, variable_name, output_qc_name
            )

        # Only assign metrics variables to the output dataset structure if requested.
        if self.keep_metrics:
            output[output_std_name] = avg_ds[output_std_name]
            output[output_goodfrac_name] = avg_ds[output_goodfrac_name]

        # Because we are updating the output dataset structure manually, we don't need
        # (or want) to return an xarray DataArray. If we did, then the code that runs
        # through the DataConverters would assign the xr.DataArray to the output
        # structure anyways, which is probably fine, but unnecessary.
        return None

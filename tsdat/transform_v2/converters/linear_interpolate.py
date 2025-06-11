from typing import TYPE_CHECKING, Any, Optional
import xarray as xr
from ...io.base import DataConverter, RetrievedDataset
from ...utils.replace_qc_attr import replace_qc_attr
from ..interpolate.calculate_linear_interpolation import interpolate
from ..utils.create_bounds import create_bounds_from_labels
from ..utils.create_input_dataset import create_input_dataset

# Prevent any chance of runtime circular imports for typing-only imports
if TYPE_CHECKING:  # pragma: no cover
    from ...config.dataset import DatasetConfig  # pragma: no cover
    from ...io.retrievers import StorageRetriever  # pragma: no cover


class LinearInterpolate(DataConverter):
    """Saves data into the specified coordinate grid using linear interpolation."""

    # TODO: Implement "ALL" -- default to iterating over all the variable's coords
    coord: str = "time"
    """The coordinate axis this converter should be applied on. Defaults to 'time'."""

    keep_metrics: bool = True
    """If true, then transform metrics will be preserved in the output dataset as data
    variables ('<variable_name>_std' and '<variable_name>_goodfraction')."""

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

        # Coordinate variables can't be the subject of an interpolation transformation.
        # The user probably made a mistake in the config file.
        if variable_name in dataset_config.coords:
            raise ValueError(
                f"{self.__repr_name__} cannot be used for coordinate variables."
                f" Offending coord: '{variable_name}'."
            )

        # Set up dataset for transform
        dataset = create_input_dataset(
            data,
            variable_name,
            self.coord,
            dataset_config,
            retrieved_dataset,
            retriever,
            input_dataset,
            input_key,
        )
        if not dataset:
            return None
        # Get the transformation parameters. This is a dictionary that has 3 keys:
        # 'alignment', 'range', and 'width'. For each entry of those there is another
        # dictionary for each coordinate to transform with the corresponding value,
        # which will be either a string or a number.
        trans_params = retriever.parameters.trans_params.select_parameters(input_key)
        t_range = trans_params["range"][self.coord]
        assert t_range is not None

        # Get the output coordinate labels and generate the 'bounds' corresponding with
        # those labels. I say 'bounds' in quotes because these are really more related
        # to the transform logic than they are necessarily to the real coord bounds, as
        # described in the next comment block below.
        labels = retrieved_dataset.coords[self.coord].values
        bounds = create_bounds_from_labels(
            labels=labels,
            width=t_range + t_range,  # range is the half-width
            alignment="center",
        )

        # ############################################################################ #
        # Do the actual transformation and extract the information we want to keep from
        # the resulting xarray dataset

        # Do the transformation
        interp_ds = interpolate(
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
        output[variable_name] = interp_ds[variable_name]

        # List of associated variable names
        output_qc_name = f"qc_{variable_name}"
        output_std_name = f"{variable_name}_std"
        output_goodfrac_name = f"{variable_name}_goodfraction"
        retrieved_var_name = retriever.match_data_var(variable_name, input_key).name  # type: ignore

        # Only assign the qc variable to the output dataset structure if requested.
        if self.keep_qc:
            output[output_qc_name] = interp_ds[output_qc_name]
            output[variable_name] = replace_qc_attr(
                output[variable_name], retrieved_var_name, variable_name, output_qc_name
            )

        # Only assign metrics variables to the output dataset structure if requested.
        if self.keep_metrics:
            if output_std_name in interp_ds:
                output[output_std_name] = interp_ds[output_std_name]
            if output_goodfrac_name in interp_ds:
                output[output_goodfrac_name] = interp_ds[output_goodfrac_name]

        # Because we are updating the output dataset structure manually, we don't need
        # (or want) to return an xarray DataArray. If we did, then the code that runs
        # through the DataConverters would assign the xr.DataArray to the output
        # structure anyways, which is probably fine, but unnecessary.
        return None

from tsdat.tstring import Template

COORDINATE_SYSTEM = "coord_sys"
INPUT_DATASTREAM = "input_ds"
OUTPUT_DATASTREAM = "output_ds"

InputKey = str
VarName = str

DATASTREAM_TEMPLATE = Template(
    "{location_id}.{dataset_name}[-{qualifier}][-{temporal}].{data_level}"
)

FILENAME_TEMPLATE = Template(
    "{datastream}.{start_date}.{start_time}[.{title}].{extension}"
)

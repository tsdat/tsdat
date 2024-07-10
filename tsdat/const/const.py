from tsdat.tstring import Template

COORDINATE_SYSTEM = "coord_sys"
INPUT_DATASTREAM = "input_ds"
OUTPUT_DATASTREAM = "output_ds"

InputKey = str
VarName = str


FILENAME_TEMPLATE = Template(
    "{datastream}.{yyyy}{mm}{dd}.{HH}{MM}{SS}[.{title}].{extension}"
)

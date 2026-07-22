import pathlib

files = [
    "tsdat/qc/checkers/check_array_mask_threshold.py",
    "tsdat/qc/checkers/check_delta.py",
    "tsdat/qc/checkers/check_monotonic.py",
    "tsdat/qc/checkers/oceanography/check_goring_nikora_2002.py",
    "tsdat/qc/handlers/cubic_spline_interp.py",
    "tsdat/qc/handlers/fail_pipeline.py",
    "tsdat/qc/handlers/sort_dataset_by_coordinate.py",
    "tsdat/qc/handlers/record_quality_results.py",
]
for f in files:
    p = pathlib.Path(f)
    txt = p.read_text()
    orig = txt
    txt = txt.replace("from pydantic import BaseModel, Extra, Field", "from pydantic import BaseModel, ConfigDict, Field")
    txt = txt.replace("from pydantic import BaseModel, Extra, root_validator, validator",
                      "from pydantic import BaseModel, ConfigDict, field_validator, model_validator")
    txt = txt.replace("from pydantic import BaseModel, Extra", "from pydantic import BaseModel, ConfigDict")
    old_forbid = "class Parameters(BaseModel, extra=Extra.forbid):\n"
    new_forbid = "class Parameters(BaseModel):\n        model_config = ConfigDict(extra='forbid')\n"
    old_allow = "class Parameters(BaseModel, extra=Extra.allow):\n"
    new_allow = "class Parameters(BaseModel):\n        model_config = ConfigDict(extra='allow')\n"
    txt = txt.replace(old_forbid, new_forbid)
    txt = txt.replace(old_allow, new_allow)
    if txt != orig:
        p.write_text(txt)
        print("Patched " + f)
    else:
        print("No change " + f)

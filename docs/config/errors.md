# Common Errors and Warnings

## Errors

### `KeyError ['time']`

Time is typically the first variable tsdat looks for, so if it can't load your dataset or if the time coordinate is not
input correctly, this error will pop up. If you have double-checked that the input file exists, then the most likely
cause of this error is in the `retriever.yaml` configuration file for the pipeline:

* double check the regex pattern in the `readers:` section to ensure that it matches the input filepath.
* double check the name of the `time` variable in the input file matches the `name:` field for the `time` coordinate.

### `Can't find module "pipeline"`

There are many modules and classes named "pipeline" in tsdat. This error typically refers to a classname specified in
the config file, i.e. `pipelines.custom_pipeline_tutorial.qc.<custom_qc>` or
`pipelines.custom_pipeline_tutorial.readers.<custom_reader>`. Make sure this classname path is correct.

### `check_<function> failed`

Ensure all the variables listed under a quality management group can be run through the function. For example, if I try
to run the test `CheckMonotonic` on all "COORDS", and one of my coordinate variables is a string array (e.g.,
`'direction': ['x','y','z']`, this function will fail). Fix this by replacing "COORDS" with only numeric coordinates
(e.g. 'time') or add the failing coordinate to the exclude section.

### `CheckMonotonic` fails for "X" values in variable `time`

If a timestamp isn't sequentially increasing, this check will fail the entire pipeline and tell you in the error message
which timestamps have failed (which timestamps are earlier in time than the previous timestamp). This is typically due
to a data logger writing error. The only built-in fix for this in the pipeline is to change the handler from
`FailPipeline` to `RemoveFailedValues`, which will drop the suspect timestamps and leave a gap. Otherwise the timestamps
will need to be fixed manually (e.g., in the `hook_customize_dataset(...)` function), assuming the missing timestamps
are recoverable.

## Warnings

### "Converting non-nanosecond precision datetime values..."

```txt
UserWarning: Converting non-nanosecond precision datetime values to nanosecond
precision. This behavior can eventually be relaxed in xarray, as it is an artifact from
pandas which is now beginning to support non-nanosecond precision values. This warning
is caused by passing non-nanosecond np.datetime64 or np.timedelta64 values to the
DataArray or Variable constructor; it can be silenced by converting the values to
nanosecond precision ahead of time.
```

This warning message shows up when the `dtype` for the `time` variable in the `dataset.yaml` configuration file is
`datetime64[s]` or `datetime64[ms]`. To fix, just change the `dtype` to `datetime64[ns]`.

## Other Issues

### If a QC handler doesn't appear to be running on a variable

* make sure it's not being overridden by another in the same pipeline
* make sure your custom QC tests are running on a single variable at a time and not affecting the entire dataset.

### Pipeline is "`skipped`"

Make sure your regex pattern in `pipeline.yaml` matches your filename. There are regex file match checkers online for a
sanity check.

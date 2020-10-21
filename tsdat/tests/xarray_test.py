import numpy as np
import xarray as xr
import pandas as pd
#from nptdms import TdmsFile

"""
This is a scratch pad for playing around with the xarray API.  Ultimately it
should be moved as it is not an official unit test.
"""

# Open one of the xarray test datasets
# xr.tutorial.open_dataset('rasm').load()


# create dummy dataframe
times = [1498867200, 1498867260, 1498867320]
heights = [1, 2]

# create dataset
ds = xr.Dataset({
    'SWdown': xr.DataArray(
                data   = np.array([[1, 2], [3, 4], [5, 6]], np.int32),
                dims   = ['time', 'height'],
                coords = {'time': times, 'height': heights},
                attrs  = {
                    '_FillValue': -9999,
                    'units'     : 'W/m2'
                    }
                ),
    'LWdown': xr.DataArray(
                data   = np.random.random(3),   # enter data here
                dims   = ['time'],
                coords = {'time': times},
                attrs  = {
                    '_FillValue': -9999.0,
                    'units'     : 'W/m2'
                    }
                )
            },
        attrs = {'example_attr': 'this is a global attribute'}
    )

for dim_name in ds.SWdown.dims:
    print(len(ds.get(dim_name)))

#---------------------------------------------------------
# The long way to get/set values for a variable
# Get n-dimensional array of values for the variable
values = ds.get('SWdown').values

print("Original values:")
for x in np.nditer(values):
   print(f"value = {x}")
   print(f"type = {type(x)}")

for x in np.nditer(values, op_flags = ['readwrite']):
   x[...] = 2*x

#---------------------------------------------------------
# The easy way to get/set values for a variable
print(ds['SWdown'])
ds['SWdown'] = ds['SWdown'] * 2
print(ds['SWdown'])

qc_var = ds.get('qc_SWdown')
if not qc_var:
    qc_var = xr.zeros_like(ds['SWdown'], np.int32)
    qc_var.attrs.clear() # clear old values
    qc_var.attrs['data_type'] = np.int32
    ds['qc_SWdown'] = qc_var

#ds['qc_SWdown'][0][0] = 1 if ds['SWdown'][0][0] > 6 else 0
print(ds['qc_SWdown'])
#
# print("Original values:")
# for x in np.nditer(values):
#    print(f"value = {x}")
#    print(f"type = {type(x)}")
#
# for x in np.nditer(values, op_flags = ['readwrite']):
#    x[...] = 2*x
#
# print("New values:")
# values = ds.get('SWdown').values
# for x in np.nditer(values):
#    print(f"value = {x}")
#
# var = ds.get('SWdown')
# qc_var = ds.get('qc_SWdown')
# if not qc_var:
#     qc_var = xr.zeros_like(var, np.int32)
#     qc_var.attrs.clear() # clear old values
#     qc_var.attrs['data_type'] = np.int32
#     ds['qc_SWdown'] = qc_var
#
#
# def get_value_at_position(dataset, var_name, position):
#     var_iter = np.nditer(dataset.get(var_name).values)
#
#     with var_iter:
#         while not var_iter.finished:
#             if var_iter.iterindex == position:
#                 return var_iter[0].item();
#             var_iter.iternext()
#
#     return None
#
#
# def test(variable_name, val, prev_value, position, dataset):
#     if val < 6:
#         return True
#     return False
#
#
# qc_var = ds.get('qc_SWdown')
# qc_iter = np.nditer(qc_var.values, op_flags = ['readwrite'])
# with qc_iter:
#     previous_value = None
#     value = None
#     for x in np.nditer(var.values):
#         value = x.item()
#         if test('SWdown', value, previous_value, qc_iter.iterindex, ds) == False:
#            qc_iter[0] = qc_iter[0] | 1
#         qc_iter.iternext()
#         previous_value = value
#
#
# print("QC var:")
# print(qc_var)
#
# value = get_value_at_position(ds, 'SWdown', 4)
# print(f"value at position 4 = {value}")

time = ds['time']
print(time)

for key in ds.coords.dims.keys():
    print(key)
    print(type(key))

sizes = ds['SWdown'].sizes

# for key in sizes:
#     dim = key
#     size = sizes[key]
#     print(dim)
#     print(size)

swdown = ds.get('SWdown')
#print(swdown.values[0][0])

print(swdown)
x = 1 # time index that failed
print(swdown[1][...].values)

#ds.where(ds.time == 1)

for variable_name in ds.data_vars:
    variable = ds.get(variable_name)
    for dim in variable.sizes:
        if dim == 'height':
            print(f"variable with dimension height = {variable.name}")

# get the total number of data points for a variable
ds.get('SWdown').size
ds.get('SWdown')[0][0].roll()
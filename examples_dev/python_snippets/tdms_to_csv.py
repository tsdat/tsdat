import numpy as np
import pandas as pd
import boto3
from nptdms import TdmsFile
import io
from io import StringIO, BytesIO, TextIOWrapper # python3; python2: BytesIO
import gzip
########################################################################
## creates s3 connection
s3 = boto3.client("s3")
s3_resource = boto3.resource('s3')
bucket = 'mhk-datalake-test'
my_bucket = s3_resource.Bucket('mhk-datalake-test')

##creates list of files names from s3 bucket
tdms_filenames = []

for object_summary in my_bucket.objects.filter(Prefix='tdms/'):
    if object_summary.key.endswith('.tdms'):
        tdms_filenames.append(object_summary.key)
        #print(object_summary.key)

dict_tdms = {}
attr_dict = {}
csv_filenames = []
fname_list = []
for files in tdms_filenames:
    key_name = files
    fname = files.replace('tdms/', '')
    fname_list.append(fname)
    csv_filenames.append(fname.split('.tdms')[0].split('_')[-1]+'.csv')

    ## this loads tdms files straight from s3bucket
    tfile = io.BytesIO(s3.get_object(
        Bucket=bucket, Key=key_name)['Body'].read())
    tdms_file = TdmsFile(tfile)
    group = tdms_file.groups()
    #print(group)
    dict_tdms[fname.split('.tdms')[0].split(
        '_')[-1]] = tdms_file.object(group[0]).as_dataframe()
    attr_dict[fname.split('.tdms')[0].split(
        '_')[-1]] = tdms_file.object(group[0]).properties
############################
check_dict = {}

for column_name in PowRaw_df.columns:
    if 'MODAQ_V' in column_name:
        my_array = pd.array(PowRaw_df[column_name])
        my_qc = pd.array(np.zeros(len(my_array)))

        my_qc[(my_array >= -11860) & (my_array <= 11860)] = 0
        my_qc[(my_array <= -11860)] = 1
        my_qc[(my_array >= 11860)] = 2

        #my_qc=my_qc.astype('bool')

        check_dict['QC_'+column_name] = pd.Series(my_qc)

    if 'MODAQ_I' in column_name:
        my_array = pd.array(PowRaw_df[column_name])
        my_qc = pd.array(np.zeros(len(my_array)))

        my_qc[(my_array >= -20) & (my_array <= 20)] = 0
        my_qc[(my_array <= -20)] = 1
        my_qc[(my_array >= 20)] = 2

        #my_qc=my_qc.astype('bool')
        check_dict['QC_'+column_name] = pd.Series(my_qc)
    if 'Time' in column_name:
        continue

a_di = dict(dict_tdms['PowRaw'])
dic2 = pd.DataFrame(dict(a_di, **check_dict))
PowRaw_df = dic2
###########################
region = boto3.Session().region_name
bucket = 'mhk-datalake-test'  # Replace with your s3 bucket name
prefix = 'csv/'
# The URL to access the bucket
bucket_path_url = 'https://mhk-datalake-test.s3-us-west-2.amazonaws.com/csv/'.format(
    region, bucket)

dir_name = 'PowRaw/'
##upload csv.gz
gz_buffer = BytesIO()
with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
    PowRaw_df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)

s3_resource.Object(bucket, prefix + dir_name +
                   'PowRaw_df.csv.gz').put(Body=gz_buffer.getvalue())

dir_name = 'Pow10Hz/'
##upload csv.gz
gz_buffer = BytesIO()
with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
    Pow10Hz_df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)

s3_resource.Object(bucket, prefix + dir_name +
                   'Pow10Hz_df.csv.gz').put(Body=gz_buffer.getvalue())

dir_name = 'GPS/'
##upload csv.gz
gz_buffer = BytesIO()
with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
    GPS_df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)

s3_resource.Object(bucket, prefix + dir_name +
                   'GPS_df.csv.gz').put(Body=gz_buffer.getvalue())

#dir_name='DMS/'
##upload csv.gz
#gz_buffer = BytesIO()
#with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
#    DMS_df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)
#
#s3_resource.Object(bucket, prefix + dir_name + 'DMS_df.csv.gz').put(Body=gz_buffer.getvalue())

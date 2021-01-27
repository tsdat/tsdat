from unittest import TestCase
import os
import shutil
from tsdat.io import DatastreamStorage, FilesystemStorage

class TestFilesystemStorage(TestCase):

    def setUp(self):
        self.basedir = os.path.abspath(os.path.dirname(__file__))
        self.root = os.path.join(self.basedir, 'data/storage/root')
        os.makedirs(self.root, exist_ok=True)

        self.temp = os.path.join(self.basedir, 'data/storage/temp')
        os.makedirs(self.temp, exist_ok=True)

        self.storage = FilesystemStorage(self.root)

    def tearDown(self) -> None:
        super().tearDown()
        shutil.rmtree(self.root)
        shutil.rmtree(self.temp)

    def test_storage(self):
        # make sure storage is empty
        assert self.storage.exists('humboldt_ca.buoy_z05.b1', '20200925', '20200926') is False

        # add one file
        file = os.path.join(self.basedir, 'data/storage/humboldt_ca.buoy_z05.b1.20200925.000000.nc')
        self.storage.save(file)

        # make sure the file exists on the file system
        assert os.path.exists(os.path.join(self.root, 'humboldt_ca/humboldt_ca.buoy_z05.b1/humboldt_ca.buoy_z05.b1.20200925.000000.nc'))

        # make sure that storage recognizes the file exists
        assert self.storage.exists('humboldt_ca.buoy_z05.b1', '20200925', '20200926',
                                   filetype=DatastreamStorage.FILE_TYPE.NETCDF)
        # add a second file
        file = os.path.join(self.basedir, 'data/storage/humboldt_ca.buoy_z05.b1.20200925.000000.nc')
        self.storage.save(file, new_filename='humboldt_ca.buoy_z05.b1.20200926.000000.nc')

        # Add a raw file
        file = os.path.join(self.basedir, 'data/storage/humboldt_ca.buoy_z05.b1.20200925.000000.nc.raw.buoy.z05.00.20200925.000000.gill.csv')
        self.storage.save(file)

        # Add an image file
        file = os.path.join(self.basedir, 'data/storage/humboldt_ca.buoy_z05.b1.20200925.000000.some_plot.png')
        self.storage.save(file)

        # check that there are 2 netcdf files in storage
        files = self.storage.find('humboldt_ca.buoy_z05.b1', '20200925', '20200930',
                          filetype=DatastreamStorage.FILE_TYPE.NETCDF)
        assert len(files) == 2

        # check there is 1 raw file in storage
        files = self.storage.find('humboldt_ca.buoy_z05.b1', '20200925', '20200930',
                          filetype=DatastreamStorage.FILE_TYPE.RAW)
        assert len(files) == 1

        # check there is 1 image file in storage
        files = self.storage.find('humboldt_ca.buoy_z05.b1', '20200925', '20200930',
                          filetype=DatastreamStorage.FILE_TYPE.PLOTS)
        assert len(files) == 1

        # Fetch one storage netcdf file to the local filesystem
        with self.storage.fetch('humboldt_ca.buoy_z05.b1', '20200925', '20200926', local_path=self.temp,
                                filetype=DatastreamStorage.FILE_TYPE.NETCDF) as fetched_files:
            # make sure the file was fetched
            assert os.path.exists(os.path.join(self.temp, 'humboldt_ca.buoy_z05.b1.20200925.000000.nc'))
            assert len(fetched_files) == 1

        # make sure the context manager got rid of the temp file
        assert os.path.exists(os.path.join(self.temp, 'humboldt_ca.buoy_z05.b1.20200925.000000.nc')) is False

        # Remove one file from storage
        self.storage.delete('humboldt_ca.buoy_z05.b1', '20200925', '20200926')

        # Assert that only one file is left in storage
        files = self.storage.find('humboldt_ca.buoy_z05.b1', '20200925', '20200930',
                          filetype=DatastreamStorage.FILE_TYPE.NETCDF)
        assert len(files) == 1

    def test_tmp_storage(self):
        # Extract a zip file into the temp area
        zipfile = os.path.join(self.basedir, 'data/storage/storage.zip')
        temp_filepath = None

        with self.storage.tmp.extract_files(zipfile) as extracted_files:
            assert len(extracted_files) == 3
            assert os.path.exists(extracted_files[0])
            temp_filepath = extracted_files[0]

        assert os.path.exists(temp_filepath) is False

        # Extract a tar.gz file into the temp area
        zipfile = os.path.join(self.basedir, 'data/storage/storage.tar.gz')
        temp_filepath = None

        with self.storage.tmp.extract_files(zipfile) as extracted_files:
            assert len(extracted_files) == 3
            assert os.path.exists(extracted_files[0])
            temp_filepath = extracted_files[0]

        assert os.path.exists(temp_filepath) is False
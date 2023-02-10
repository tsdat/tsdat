import os
import re
from subprocess import check_output, CalledProcessError
from setuptools import setup, find_packages, Extension
from pathlib import Path
from Cython.Build import cythonize
import numpy

README = Path("README.md").read_text()

requirements = Path("requirements.txt").read_text().strip().splitlines()
REQUIREMENTS = [req for req in requirements if not req.startswith("#")]

VERSION = os.environ["TSDAT_VERSION"]

def pkgconfig(lib, opt):
    res = check_output(["pkg-config", opt, lib]).decode('utf-8').strip()
    return [re.compile(r'^-[ILl]').sub('', m) for m in res.split()]

cds3_incdirs = pkgconfig("cds3", '--cflags-only-I')
cds3_libdirs = pkgconfig("cds3", '--libs-only-L')
cds3_libs    = pkgconfig("cds3", '--libs-only-l')

cds3_incdirs.append(numpy.get_include())

cds3 = Extension(
    name            = 'cds3.core',
    sources         = ['cds3/core.pyx'],
    include_dirs    = cds3_incdirs,
    library_dirs    = cds3_libdirs,
    libraries       = cds3_libs,
    runtime_library_dirs = cds3_libdirs
)

cds3_enums = Extension(
    name            = 'cds3.enums',
    sources         = ['cds3/enums.pyx'],
    include_dirs    = cds3_incdirs,
    library_dirs    = cds3_libdirs,
    libraries       = cds3_libs,
    runtime_library_dirs = cds3_libdirs
)

setup(
    name="tsdat",
    version=VERSION,
    description="A data processing framework used to convert time series data into standardized format.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/tsdat/tsdat",
    author="tsdat",
    author_email="tsdat@pnnl.gov",
    license="Simplified BSD (2-clause)",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
    ],
    ext_modules=cythonize([cds3,cds3_enums]),
    packages=find_packages(exclude=["test"]),
    package_data={"tsdat": ["py.typed"]},
    include_package_data=True,
    python_requires=">=3.8",
    zip_safe=False,
    install_requires=REQUIREMENTS,
    entry_points={"console_scripts": ["tsdat = tsdat.main:app"]},
    scripts=[],
)


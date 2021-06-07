from os import path
import pathlib
from setuptools import setup, find_packages

# The directory containing this file
CWD = pathlib.Path(__file__).parent

# The text of the README file
README = (CWD / "README.md").read_text()

# Get the list of dependencies from the requirements.txt file
with open(path.join(CWD, 'requirements.txt')) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    REQUIREMENTS = [line for line in requirements_file.read().splitlines()
                    if not line.startswith('#')]

setup(
    name="tsdat",
    version="0.2.4",
    description="A data processing framework used to convert time series data into standardized format.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/tsdat/tsdat",
    author="Carina Lansing <carina.lansing@pnnl.gov>, Maxwell Levin <maxwell.levin@pnnl.gov>",
    author_email="carina.lansing@pnnl.gov",
    license="Simplified BSD (2-clause)",
    classifiers=[
        "Development Status :: 3 - Alpha",
        'Natural Language :: English',
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent"
    ],
    packages=find_packages(exclude=['docs', 'tests', 'examples', 'examples_dev']),
    entry_points={'console_scripts': []},
    include_package_data=True,
    zip_safe=False,
    install_requires=REQUIREMENTS,
    scripts=[]
)

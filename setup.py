import os
import pathlib
import setuptools

# The directory containing this file
CWD = pathlib.Path(__file__).parent

# The text of the README file
README = (CWD / "README.md").read_text()

# Get the list of dependencies from the requirements.txt file
with open(os.path.join(CWD, "requirements.txt")) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    REQUIREMENTS = [
        line
        for line in requirements_file.read().splitlines()
        if not line.startswith("#")
    ]

try:
    version = os.environ["TSDAT_VERSION"]
    assert "." in version
except:
    version = {}

setuptools.setup(
    name="tsdat",
    version=version,
    description="A data processing framework used to convert time series data into standardized format.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/tsdat/tsdat",
    author="Carina Lansing, Maxwell Levin",
    author_email="tsdat@pnnl.gov",
    license="Simplified BSD (2-clause)",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(exclude=["docs", "tests", "examples"]),
    entry_points={"console_scripts": []},
    include_package_data=True,
    zip_safe=False,
    install_requires=REQUIREMENTS,
    scripts=[],
)

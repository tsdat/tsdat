import os
import setuptools
from pathlib import Path

README = Path("README.md").read_text()

requirements = Path("requirements.txt").read_text().strip().splitlines()
REQUIREMENTS = [req for req in requirements if not req.startswith("#")]

VERSION = os.environ["TSDAT_VERSION"]


setuptools.setup(
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
    packages=setuptools.find_packages(exclude=["test"]),
    package_data={"tsdat": ["py.typed"]},
    include_package_data=True,
    python_requires=">=3.8",
    zip_safe=False,
    install_requires=REQUIREMENTS,
    entry_points={"console_scripts": ["tsdat = tsdat.main:app"]},
    scripts=[],
)

import os
import sys

from setuptools import find_packages, setup

from version import get_version


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


# Only install black on Python 3.6 or higher
maybe_black = []
if sys.version_info > (3, 6):
    maybe_black = ["black"]

setup(
    name="digital-land",
    version=get_version(),
    description="Data pipeline tools to collect data and process it into a dataset",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="DLUHC Digital Land Team",
    author_email="DigitalLand@levellingup.gov.uk",
    license="MIT",
    url="https://github.com/digital-land/digital-land-python",
    packages=find_packages(exclude="tests"),
    package_data={"digital-land": ["templates/*.html"]},
    include_package_data=True,
    install_requires=[
        "datasette",
        "canonicaljson",
        "click",
        "cchardet",
        "esridump",
        "pandas",
        "pyproj",
        "requests",
        "validators",
        "xlrd==1.2.0",
        "openpyxl",
        "Shapely==1.8.0",  # Version locking till https://github.com/shapely/shapely/issues/1364 is fixed
        "SPARQLWrapper",
        "geojson",
        "spatialite",
        "pyyaml",
        "dataclasses-json",
    ],
    entry_points={"console_scripts": ["digital-land=digital_land.cli:cli"]},
    setup_requires=["pytest-runner"],
    extras_require={
        "test": [
            "coverage",
            "flake8",
            "pytest",
            "coveralls",
            "twine",
            "responses",
            "XlsxWriter",
            "wasabi",
        ]
        + maybe_black
    },
    tests_require=["openregister[test]", "responses"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)

from setuptools import setup, find_packages
import os
import sys
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
    description="Python tools to collect and convert CSV and other resources into a Digital Land dataset",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="MHCLG Digital Land Team",
    author_email="DigitalLand@communities.gov.uk",
    license="MIT",
    url="https://github.com/digital-land/pipeline",
    packages=find_packages(exclude="tests"),
    package_data={"digital-land": ["templates/*.html"]},
    include_package_data=True,
    install_requires=[
        "click==7.0",
        "cchardet==2.1.5",
        "pandas==1.0.1",
        "pyproj==2.4.2.post1",
        "xlrd==1.2.0",
    ],
    entry_points={"console_scripts": ["digital-land=digital_land.cli:cli",]},
    setup_requires=["pytest-runner"],
    extras_require={
        "test": [
            "coverage>=4.5.3",
            "pytest>=4.0.2",
            "python-coveralls>=2.9.1",
            "twine>=1.13.0",
        ]
        + maybe_black
    },
    tests_require=["openregister[test]"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)

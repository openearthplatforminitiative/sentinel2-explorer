import re
from io import open

from setuptools import find_packages, setup

with open("sentinelsat/__init__.py", encoding="utf-8") as f:
    version = re.search(r'__version__\s*=\s*"(\S+)"', f.read()).group(1)

setup(
    name="sentinel2-explorer",
    version=version,
    description="Utility for finding and downloading sentinel2 imagery, forked from sentinelsat and sentinelloader",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: GIS",
        "Topic :: Utilities",
    ],
    url="https://github.com/openearthplatforminitiative/sentinel2-explorer",
    license="Apache 2.0",
    packages=find_packages(),
    include_package_data=False,
    zip_safe=True,
    install_requires=open("requirements.txt").read().splitlines(),
    extras_require={
        "dev": [
            "pandas",
            "geopandas",
            "shapely",
            "pytest >= 3.6.3",
            "pytest-vcr",
            "pytest-socket",
            "requests-mock",
            "pyyaml",
            "rstcheck < 6",
            "sphinx >= 1.3",
            "sphinx_rtd_theme",
            "flaky",
        ],
    },
)
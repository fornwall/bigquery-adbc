from __future__ import annotations

from pathlib import Path

from setuptools import find_packages, setup

from _build_support import BinaryDistribution, bdist_wheel, build_py, cargo_version, setup_options

setup(
    version=cargo_version(Path(__file__).resolve().parent.parent / "Cargo.toml"),
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    package_data={"bigquery_adbc": ["py.typed"]},
    distclass=BinaryDistribution,
    cmdclass={"bdist_wheel": bdist_wheel, "build_py": build_py},
    options=setup_options(),
)

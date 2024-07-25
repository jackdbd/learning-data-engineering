from setuptools import find_packages, setup

setup(
    name="nyc_311",
    packages=find_packages(exclude=["nyc_311_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

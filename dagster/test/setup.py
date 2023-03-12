from setuptools import find_packages, setup

setup(
    name="test",
    packages=find_packages(exclude=["test_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)

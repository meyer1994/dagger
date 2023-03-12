from setuptools import find_packages, setup

setup(
    name="b3_acoes",
    packages=find_packages(exclude=["b3_acoes_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)

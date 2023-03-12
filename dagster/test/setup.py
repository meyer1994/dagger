from setuptools import find_packages, setup

setup(
    name="test",
    packages=find_packages(exclude=["test_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas==1.5.3",
        "s3fs==2023.3.0",
        "pangres==4.1.3",
        "pyarrow==11.0.0",
        "SQLAlchemy==1.4.46",
        "psycopg2==2.9.5",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)

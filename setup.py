from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

short_description = (
    "PyCosmosDal is a wrapper around the Microsoft Azure CosmosDb Python SDK."
)

setup(
    install_requires=["azure-cosmos==3.1.2"],
    name="pycosmosdal-teqniqly",
    version="0.0.1",
    author="Teqniqly",
    author_email="farooq@teqniqly.com",
    description=short_description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/teqniqly/py-cosmos-dal",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)

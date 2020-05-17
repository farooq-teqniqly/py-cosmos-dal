import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

short_description = (
    "PyCosmosDal is a wrapper around the Microsoft Azure CosmosDb Python SDK."
)

setuptools.setup(
    name="pycosmosdal-teqniqly",  # Replace with your own username
    version="0.0.1",
    author="Teqniqly",
    author_email="farooq@teqniqly.com",
    description=short_description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/teqniqly/py-cosmos-dal",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)

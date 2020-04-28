import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Pdp",
    version="0.0.1",
    author="PDP Team",
    description="A package that makes pipelining Python jobs easier",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DanielKrolopp/PythonDataPipeline",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    python_requires='>=3.6',
)

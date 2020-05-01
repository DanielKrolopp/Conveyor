import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="parallel-conveyor",
    version="0.0.2",
    author="Conveyor Team",
    description="Simple, intuitive pipelining in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DanielKrolopp/Conveyor",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    python_requires='>=3.8',
)

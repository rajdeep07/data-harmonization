import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="deduplication_benchmark",
    version="0.1.2",
    author="Rajdeep Arora",
    author_email="rajdeep@tigeranalytics.com",
    description="This package will run de-duplication for the groups",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)




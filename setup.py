import os

from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)
install_requires = [line.rstrip() for line in open(os.path.join(base_dir, 'requirements.in'))]

with open("README.md", "r", encoding="utf-8") as md:
    long_description = md.read()

setup(
    name='hca-ingest',
    version='2.7.2',
    description='A library to communicate with the Human Cell Atlas ingest API hosted by the EBI for creation and management of HCA project submissions.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ebi-ait/ingest-client",
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=install_requires,
    include_package_data=True
)

# see https://github.com/pypa/sampleproject/blob/master/setup.py
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# not recommended since requirements.txt is overly restrictive, but whatever
with open(path.join(here, 'jassrealtime', 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().splitlines()

with open("VERSION", encoding='utf-8') as f:
    version = f.read()

setup(
    name='RACS',
    version=version,
    description='Repository for Annotations, Corporas and Schemas',
    long_description=long_description,
    author='CRIM',
    author_email='support@crim.ca',
    url='http://TODO.com',

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Developers'


        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6',
    ],

    keywords='annotation storage elasticsearch',
    packages=find_packages(),
    package_data={
        'jassrealtime': ['requirements.txt'],

    },
    include_package_data=True,
    install_requires=requirements

)

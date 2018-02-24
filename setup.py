from setuptools import setup, find_packages
from pgmerge import __version__
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

# Get the long description from the README file
# pandoc --from=markdown --to=rst --output=README.rst README.md
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pgmerge',
    version=__version__,
    author='Simon Muller',
    author_email='samullers@gmail.com',
    url='https://github.com/samuller/pgmerge',
    description='PostgreSQL data import/export utility',
    long_description=long_description,
    python_requires='>=3',
    py_modules=['pgmerge'],
    packages=find_packages(exclude=['*.tests*']),
    install_requires=requirements,
    package_data={'pgmerge': ['NOTICE']},
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'pgmerge=pgmerge.pgmerge:main',
        ],
    },
)

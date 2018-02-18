from setuptools import setup, find_packages
from pgmerge import __version__

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='pgmerge',
    version=__version__,
    author='Simon Muller',
    author_email='samullers@gmail.com',
    url='https://github.com/samuller/pgmerge',
    py_modules=['pgmerge'],
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'pgmerge=pgmerge.pgmerge:main',
        ],
    },
)

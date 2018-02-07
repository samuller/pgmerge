from setuptools import setup, find_packages

setup(
    name='pgmerge',
    version='0.9',
    author='Simon Muller',
    py_modules=['pgmerge'],
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'pgmerge=pgmerge.pgmerge:main',
        ],
    },
)

from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='pgmerge',
    version='0.9',
    author='Simon Muller',
    url='https://github.com/samuller/pgmerge',
    py_modules=['pgmerge'],
    packages=find_packages(),
    install_requires=requirements,
    package_data={'': ['NOTICE', 'pgmerge/default_config_schema.yml']},
    entry_points={
        'console_scripts': [
            'pgmerge=pgmerge.pgmerge:main',
        ],
    },
)

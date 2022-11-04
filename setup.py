from setuptools import setup, find_namespace_packages
import os

major_ver = 0
minor_ver = 1
rev = 0
lib_folder = os.path.dirname(os.path.realpath(__file__))
requirement_path = lib_folder + '/requirements.txt'
install_requires = []
if os.path.isfile(requirement_path):
    with open(requirement_path) as f:
        install_requires = f.read().splitlines()
  
setup(
    name='ingestion',
    description="Ingest from API and write to Postgres",
    version=f"{major_ver}.{minor_ver}.{rev}",
    python_requires='>=3.7',
    packages=find_namespace_packages(),
    install_requires = install_requires
    entry_points = {
        'scripts': [
            'tracking_ingestion = scripts.python.ingest:main'
        ]
    }
)

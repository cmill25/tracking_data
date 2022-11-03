from setuptools import setup, find_namespace_packages

majorVer = 0
minorVer = 1
rev = 0

setup(
    name='bg_apps',
    description="Data Engineering's Python jobs.",
    version="{0}.{1}.{2}".format(majorVer, minorVer, rev),
    python_requires='>=3.7',
    packages=find_namespace_packages(),
    entry_points = {
        'scripts': [
            'azure_billing_ingestion = apps.AzureBilling.ingest:main',
            'sql_discovery = apps.SQLSources.discovery:main',
            'sql_ingestion = apps.SQLSources.ingest:main'
        ]
    }
)

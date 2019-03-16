from setuptools import setup, find_packages
from kafka_worker import __version__


setup(
    name='kafka_worker',
    description='Airflow Kafka Worker',
    version=__version__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    scripts=['kafka_worker/bin/kafka_worker'],
    install_requires=[
        'apache-airflow>=1.9.0',
        'kafka>=1.3.5'
    ],
    extras_require={},
    author='Jose Plascencia',
    author_email='jose.plascencia@clairvoyantsoft.com', 'joseplascencia22@gmail.com'
)

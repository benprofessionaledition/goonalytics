from setuptools import setup
from setuptools import find_packages

setup(name='goonalytics',
      version='0.1a',
      description='Goonalytics: web scraping, machine learning and whatever with the Something Awful forums',
      author='Ben Levine',
      author_email='ben@goonalytics.io',
      url='https://goonalytics.io',
      download_url='https://github.com/benlevineprofessionaledition/goonalytics',
      license='MIT',
      install_requires=['numpy',
                        'tensorflow',
                        'networkx',
                        'scipy',
                        'scrapy',
                        'lxml',
                        'nltk',
                        'bs4',
                        'google-cloud-core',
                        'google-cloud-bigquery',
                        'google-cloud-storage',
                        'flask',
                        'elasticsearch',
                        'avro-python3'
                        ],
      package_data={'goonalytics': ['README.md']},
      packages=find_packages())
from setuptools import (
    setup,
    find_packages
)


with open('VERSION', 'r') as file_obj:
    version = file_obj.readline().strip()


setup(name='cdcagg_oai',
      version=version,
      url='https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler',
      description='',
      license='EUPL v1.2',
      author='Toni Sissala',
      author_email='toni.sissala@tuni.fi',
      packages=find_packages(exclude=['tests']),
      include_package_data=True,
      entry_points={
        'cdcagg.oai.metadataformats': [
            'AggOAIDDI25MetadataFormat = cdcagg_oai.metadataformats:AggOAIDDI25MetadataFormat',
            'AggOAIDataciteMetadataFormat = cdcagg_oai.metadataformats:AggOAIDataciteMetadataFormat',
            'AggDCMetadataFormat = cdcagg_oai.metadataformats:AggDCMetadataFormat']},
      classifiers=(
          'Development Status :: 1 - Planning',
          'Environment :: Console',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: European Union Public Licence 1.2 (EUPL 1.2)',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Topic :: Internet :: WWW/HTTP :: HTTP Servers'))

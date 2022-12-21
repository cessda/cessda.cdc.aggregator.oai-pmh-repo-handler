# Copyright CESSDA ERIC 2021
#
# Licensed under the EUPL, Version 1.2 (the "License"); you may not
# use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import (
    setup,
    find_packages
)


with open('VERSION', 'r') as file_obj:
    version = file_obj.readline().strip()


requires = [
    'kuha_common>=2.0.0',
    'kuha_oai_pmh_repo_handler>=1.2.0',
    'cdcagg_common>=0.5.0',
    'Genshi',
    'PyYaml'
]


setup(name='cdcagg_oai',
      version=version,
      url='https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler',
      description='',
      license='EUPL v1.2',
      author='Toni Sissala',
      author_email='toni.sissala@tuni.fi',
      packages=find_packages(exclude=['tests']),
      include_package_data=True,
      install_requires=requires,
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

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Apache Beam SDK for Python examples setup file."""

from distutils.command.build import build as _build
import subprocess

import setuptools

PACKAGE_NAME = 'bl-df-crawl-amz'
PACKAGE_VERSION = '0.0.1'
PACKAGE_DESCRIPTION = ''
PACKAGE_URL = 'https://github.com/BlueLens/bl-df-crawl-amz/'
PACKAGE_DOWNLOAD_URL = ''
PACKAGE_AUTHOR = 'BlueHack Inc.'
PACKAGE_EMAIL = 'master@bluehack.net'
PACKAGE_KEYWORDS = 'bluelens'
PACKAGE_LONG_DESCRIPTION = '''
'''

REQUIRED_PACKAGES = [
    'google-cloud-dataflow==2.3.0',
    'lxml==4.1.1'
    ]

REQUIRED_SETUP_PACKAGES = [
    'nose>=1.0',
    ]

REQUIRED_TEST_PACKAGES = [
    'pyhamcrest>=1.9,<2.0',
    ]

CUSTOM_COMMANDS = [
    ['echo', 'Custom command worked!'],
    ['apt-get', 'update'],
    ['apt-get', '--assume-yes', 'install', 'libxml2-dev'],
    ['apt-get', '--assume-yes', 'install', 'libxslt1-dev'],
    ['apt-get', '--assume-yes', 'install', 'python-dev'],
    ['apt-get', '--assume-yes', 'install', 'libxml2'],
    ['apt-get', '--assume-yes', 'install', 'python-lxml'],
    ['apt-get', '--assume-yes', 'install', 'lib32z1-dev'],
]

class build(_build):  # pylint: disable=invalid-name
    sub_commands = _build.sub_commands + [('CustomCommands', None)]


class CustomCommands(setuptools.Command):
    """A setuptools Command class able to run arbitrary commands."""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print 'Running command: %s' % command_list
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # Can use communicate(input='y\n'.encode()) if the command run requires
        # some confirmation.
        stdout_data, _ = p.communicate()
        print 'Command output: %s' % stdout_data
        if p.returncode != 0:
            raise RuntimeError(
                'Command %s failed: exit code: %s' % (command_list, p.returncode))

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description=PACKAGE_DESCRIPTION,
    long_description=PACKAGE_LONG_DESCRIPTION,
    url=PACKAGE_URL,
    download_url=PACKAGE_DOWNLOAD_URL,
    author=PACKAGE_AUTHOR,
    author_email=PACKAGE_EMAIL,
    packages=setuptools.find_packages(),
    setup_requires=REQUIRED_SETUP_PACKAGES,
    install_requires=REQUIRED_PACKAGES,
    test_suite='nose.collector',
    tests_require=REQUIRED_TEST_PACKAGES,
    zip_safe=False,
    # PyPI package information.
    classifiers=[
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    license='Apache License, Version 2.0',
    keywords=PACKAGE_KEYWORDS,
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    }
)

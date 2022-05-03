# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
from typing import Dict, Set
import setuptools

is_py37_or_newer = sys.version_info >= (3, 7)

package_metadata: dict = {}
with open("./src/datahub_actions/__init__.py") as fp:
    exec(fp.read(), package_metadata)


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md")) as f:
        description = f.read()

    return description


base_requirements = {
    # Compatibility.
    "dataclasses>=0.6; python_version < '3.7'",
    "typing_extensions>=3.7.4; python_version < '3.8'",
    "mypy_extensions>=0.4.3",
    # Actual dependencies.
    "typing-inspect",
    "pydantic>=1.5.1",
    "acryl-datahub>=0.8.34",
    "dictdiffer",
}

kafka_common = {
    "confluent-kafka>=1.5.0",
    "fastavro>=1.2.0",
}

framework_common = {
    "click>=6.0.0",
    "click-default-group",
    "PyYAML",
    "toml>=0.10.0",
    "entrypoints",
    "docker",
    "expandvars>=0.6.5",
    "avro-gen3==0.7.2",
    "avro>=1.10.2",
    "python-dateutil>=2.8.0",
    "stackprinter",
    "tabulate",
    "progressbar2",
    *kafka_common,
}

aws_common = {
    # AWS Python SDK
    "boto3",
    # Deal with a version incompatibility between botocore (used by boto3) and urllib3.
    # See https://github.com/boto/botocore/pull/2563.
    "botocore!=1.23.0",
}

# Note: for all of these, framework_common will be added.
plugins: Dict[str, Set[str]] = {
    # Source Plugins
    "kafka": kafka_common,
    # Action Plugins 
    "executor": { 
        f"acryl-executor>=0.0.3rc2",
    }
    # Transformer Plugins (None yet)
}

mypy_stubs = {
    "types-pytz",
    "types-dataclasses",
    "sqlalchemy-stubs",
    "types-pkg_resources",
    "types-six",
    "types-python-dateutil",
    "types-requests",
    "types-toml",
    "types-PyMySQL",
    "types-PyYAML",
    "types-freezegun",
    "types-cachetools",
    # versions 0.1.13 and 0.1.14 seem to have issues
    "types-click==0.1.12",
    "boto3-stubs[s3,glue,sagemaker]",
    "types-tabulate",
}

base_dev_requirements = {
    *base_requirements,
    *framework_common,
    *mypy_stubs,
    "black>=22.1.0",
    "coverage>=5.1",
    "flake8>=3.8.3",
    "flake8-tidy-imports>=4.3.0",
    "isort>=5.7.0",
    "mypy>=0.901,<0.920",
    "pytest>=6.2.2",
    "pytest-cov>=2.8.1",
    "pytest-docker>=0.10.3",
    "tox",
    "deepdiff",
    "requests-mock",
    "freezegun",
    "jsonpickle",
    "build",
    "twine",
    *list(
        dependency
        for plugin in [
            "kafka",
            "executor",
        ]
        for dependency in plugins[plugin]
    ),
}

dev_requirements = {
    *base_dev_requirements,
}

full_test_dev_requirements = {
    *list(
        dependency
        for plugin in [
            "kafka",
            "executor",
        ]
        for dependency in plugins[plugin]
    ),
}

entry_points = {
    "console_scripts": ["datahub-actions = datahub_actions.entrypoints:main"], 
    "datahub_actions.action.plugins": [
        "executor = datahub_actions.plugin.action.execution.executor_action:ExecutorAction",
    ],
    "datahub_actions.transformer.plugins": [],
    "datahub_actions.source.plugins": [],
}


setuptools.setup(
    # Package metadata.
    name=package_metadata["__package_name__"],
    version=package_metadata["__version__"],
    url="https://datahubproject.io/",
    project_urls={
        "Documentation": "https://datahubproject.io/docs/actions",
        "Source": "https://github.com/acryldata/datahub-actions",
        "Changelog": "https://github.com/acryldata/datahub-actions/releases",
    },
    license="Apache License 2.0",
    description="An action framework to work with DataHub real time changes.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Environment :: MacOS X",
        "Topic :: Software Development",
    ],
    # Package info.
    zip_safe=False,
    python_requires=">=3.6",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="./src"),
    package_data={
        "datahub_actions": ["py.typed"],
    },
    entry_points=entry_points,
    # Dependencies.
    install_requires=list(base_requirements | framework_common),
    extras_require={
        "base": list(framework_common),
        **{
            plugin: list(framework_common | dependencies)
            for (plugin, dependencies) in plugins.items()
        },
        "all": list(
            framework_common.union(
                *[
                    requirements
                    for plugin, requirements in plugins.items()
                ]
            )
        ),
        "dev": list(dev_requirements),
        "integration-tests": list(full_test_dev_requirements),
    },
)

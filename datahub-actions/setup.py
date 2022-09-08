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
    # The confluent_kafka package provides a number of pre-built wheels for
    # various platforms and architectures. However, it does not provide wheels
    # for arm64 (including M1 Macs) or aarch64 (Docker's linux/arm64). This has
    # remained an open issue on the confluent_kafka project for a year:
    #   - https://github.com/confluentinc/confluent-kafka-python/issues/1182
    #   - https://github.com/confluentinc/confluent-kafka-python/pull/1161
    #
    # When a wheel is not available, we must build from source instead.
    # Building from source requires librdkafka to be installed.
    # Most platforms have an easy way to install librdkafka:
    #   - MacOS: `brew install librdkafka` gives latest, which is 1.9.x or newer.
    #   - Debian: `apt install librdkafka` gives 1.6.0 (https://packages.debian.org/bullseye/librdkafka-dev).
    #   - Ubuntu: `apt install librdkafka` gives 1.8.0 (https://launchpad.net/ubuntu/+source/librdkafka).
    #
    # Moreover, confluent_kafka 1.9.0 introduced a hard compatibility break, and
    # requires librdkafka >=1.9.0. As such, installing confluent_kafka 1.9.x on
    # most arm64 Linux machines will fail, since it will build from source but then
    # fail because librdkafka is too old. Hence, we have added an extra requirement
    # that requires confluent_kafka<1.9.0 on non-MacOS arm64/aarch64 machines, which
    # should ideally allow the builds to succeed in default conditions. We still
    # want to allow confluent_kafka >= 1.9.0 for M1 Macs, which is why we can't
    # broadly restrict confluent_kafka to <1.9.0.
    #
    # Note that this is somewhat of a hack, since we don't actually require the
    # older version of confluent_kafka on those machines. Additionally, we will
    # need monitor the Debian/Ubuntu PPAs and modify this rule if they start to
    # support librdkafka >= 1.9.0.
    "confluent_kafka>=1.5.0",
    'confluent_kafka<1.9.0; platform_system != "Darwin" and (platform_machine == "aarch64" or platform_machine == "arm64")',
    # We currently require both Avro libraries. The codegen uses avro-python3 (above)
    # schema parsers at runtime for generating and reading JSON into Python objects.
    # At the same time, we use Kafka's AvroSerializer, which internally relies on
    # fastavro for serialization. We do not use confluent_kafka[avro], since it
    # is incompatible with its own dep on avro-python3.
    "fastavro>=1.2.0",
}

framework_common = {
    "click>=6.0.0",
    "click-default-group",
    "prometheus-client",
    "PyYAML",
    "toml>=0.10.0",
    "entrypoints",
    "docker",
    "expandvars>=0.6.5",
    "avro-gen3==0.7.4",
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
        "acryl-executor>=0.0.3.5",
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
                *[requirements for plugin, requirements in plugins.items()]
            )
        ),
        "dev": list(dev_requirements),
        "integration-tests": list(full_test_dev_requirements),
    },
)

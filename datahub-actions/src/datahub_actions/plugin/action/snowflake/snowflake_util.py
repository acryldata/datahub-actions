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

import logging
import re

from datahub.emitter.mce_builder import dataset_urn_to_key
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeConfig
from datahub.ingestion.source.sql.sql_config import make_sqlalchemy_uri
from sqlalchemy import create_engine

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeTagHelper:
    def __init__(self, config: SnowflakeConfig):
        self.config: SnowflakeConfig = config

    @staticmethod
    def get_label_urn_to_tag(label_urn: str) -> str:
        pattern = r"urn:li:tag:(.*)"
        results = re.search(pattern, label_urn)
        if results is not None:
            return results.group(1)
        else:
            pattern = r"urn:li:glossaryTerm:(.*)"
            results = re.search(pattern, label_urn)
            if results is not None:
                # terms use `.` for separation, replace with _
                return results.group(1).replace(".", "_")
            else:
                raise ValueError(f"Invalid tag or term urn {label_urn}")

    def apply_tag_or_term(self, dataset_urn: str, tag_or_term_urn: str) -> None:
        dataset_key = dataset_urn_to_key(dataset_urn)
        assert dataset_key is not None
        if dataset_key.platform != "snowflake":
            return
        tag = self.get_label_urn_to_tag(tag_or_term_urn)
        assert tag is not None
        name_tokens = dataset_key.name.split(".")
        assert len(name_tokens) == 3
        self.run_query(
            name_tokens[0], name_tokens[1], f"CREATE TAG IF NOT EXISTS {tag};"
        )
        self.run_query(
            name_tokens[0],
            name_tokens[1],
            f'ALTER TABLE {name_tokens[2]} SET TAG {tag}="{tag}";',
        )

    def remove_tag_or_term(self, dataset_urn: str, tag_urn: str) -> None:
        dataset_key = dataset_urn_to_key(dataset_urn)
        assert dataset_key is not None
        if dataset_key.platform != "snowflake":
            return
        tag = self.get_label_urn_to_tag(tag_urn)
        assert tag is not None
        name_tokens = dataset_key.name.split(".")
        assert len(name_tokens) == 3
        self.run_query(
            name_tokens[0],
            name_tokens[1],
            f"ALTER TABLE {name_tokens[2]} UNSET TAG {tag};",
        )

    def run_query(self, database: str, schema: str, query: str) -> None:
        try:
            self.config.get_sql_alchemy_url()
            uri_opts = {
                key: value
                for (key, value) in {
                    "warehouse": self.config.warehouse,
                    "role": self.config.role,
                    "schema": schema,
                }.items()
                if value
            }
            url = make_sqlalchemy_uri(
                self.config.scheme,
                self.config.username,
                self.config.password.get_secret_value()
                if self.config.password
                else None,
                self.config.host_port,
                database,
                uri_opts=uri_opts,
            )
            engine = create_engine(url, **self.config.options)
            engine.execute(query)
            logger.info(f"Successfully executed query{query}")
        except Exception as e:
            logger.warning(
                f"Failed to execute snowflake query: {query}. Exception: ", e
            )

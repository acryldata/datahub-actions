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

# Defining environment
ARG APP_ENV=prod

FROM acryldata/datahub-ingestion-base:head-full as prod-install
COPY datahub-actions /actions-src

USER root

RUN mkdir -p /etc/datahub/actions && mkdir -p /tmp/datahub/logs/actions/system
RUN cd /actions-src && \
    pip install "." && \
    pip install '.[all]' && \
    # This is required to fix security vulnerability in htrace-core4
    rm -f /usr/local/lib/python3.10/site-packages/pyspark/jars/htrace-core4-4.1.0-incubating.jar

COPY ./docker/datahub-actions/start.sh /start_datahub_actions.sh
COPY ./docker/datahub-actions/readiness-check.sh /readiness-check.sh
RUN chmod a+x /start_datahub_actions.sh

# Add other default configurations into this!
RUN mkdir -p /etc/datahub/actions/conf && mkdir -p /etc/datahub/actions/system/conf
COPY ./docker/config/*.yaml /etc/datahub/actions/system/conf

# datahub:datahub is created in datahub-ingestion-base image
RUN chown datahub:datahub /etc/datahub && \
    chown -R datahub:datahub /tmp/datahub

# By transferring the root user's pip cache directory to the datahub
# user, we can avoid the need for some redundant dependency downloads.
RUN mkdir -p /home/datahub/.cache \
    && mv /root/.cache/pip /home/datahub/.cache/pip \
    && chown -R datahub:datahub /home/datahub/.cache/pip

FROM ${APP_ENV}-install as final
USER datahub
RUN curl -s "https://get.sdkman.io" | bash
RUN /bin/bash -c "source /$HOME/.sdkman/bin/sdkman-init.sh; sdk version; sdk install java 8.0.332-zulu"
ENTRYPOINT [ ]
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh

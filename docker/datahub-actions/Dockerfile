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

FROM acryldata/datahub-ingestion-base:latest as prod-install
COPY datahub-actions /actions-src
RUN mkdir -p /etc/datahub/actions && mkdir -p /tmp/datahub/logs/actions/system
RUN cd /actions-src && \
    pip install "." && \
    pip install '.[all]'

COPY ./docker/datahub-actions/start.sh /start_datahub_actions.sh
RUN chmod a+x /start_datahub_actions.sh

# Add other default configurations into this!
RUN mkdir -p /etc/datahub/actions/conf && mkdir -p /etc/datahub/actions/system/conf
COPY ./docker/config/*.yaml /etc/datahub/actions/system/conf

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub \
    && chown datahub /etc/datahub \
    && chown -R datahub /tmp/datahub

# By transferring the root user's pip cache directory to the datahub
# user, we can avoid the need for some redundant dependency downloads.
RUN mkdir -p /home/datahub/.cache \
    && mv /root/.cache/pip /home/datahub/.cache/pip \
    && chown -R datahub /home/datahub/.cache/pip

FROM ${APP_ENV}-install as final
USER datahub
RUN curl -s "https://get.sdkman.io" | bash
RUN /bin/bash -c "source /$HOME/.sdkman/bin/sdkman-init.sh; sdk version; sdk install java 8.0.332-zulu"
ENV DATAHUB_GMS_PROTOCOL http
CMD dockerize -wait ${DATAHUB_GMS_PROTOCOL:-http}://$DATAHUB_GMS_HOST:$DATAHUB_GMS_PORT/health -timeout 240s /start_datahub_actions.sh

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

# Public Acryl-Actions Container. 

# Defining environment
ARG APP_ENV=prod

FROM python:3.9.9 as base
ENV DOCKERIZE_VERSION v0.6.1
RUN apt-get update \
&& if [ $(arch) = "aarch64" ]; then \
       DOCKERIZE_ARCH='aarch64';\
        elif [ $(arch) = "x86_64" ]; then \
       DOCKERIZE_ARCH='amd64'; \
        else \
        echo >&2 "Unsupported architecture $(arch)" ; exit 1; \
        fi \
        && apt-get install -y \
        jq \
        librdkafka-dev \
        python3-ldap \
        libldap2-dev \
        libsasl2-dev \
        libsasl2-modules \
        ldap-utils \
    && curl -L https://github.com/treff7es/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-${DOCKERIZE_ARCH}-$DOCKERIZE_VERSION.tar.gz | tar -C /usr/local/bin -xzv \
    && python -m pip install --upgrade pip wheel setuptools==57.5.0

FROM base as prod-install
COPY datahub_actions /actions-src
RUN mkdir -p /etc/datahub/actions
RUN cd /actions-src && \
    pip install "." && \
    pip install '.[executor]'
    
COPY ./docker/datahub_actions/start.sh /start_datahub_actions.sh
RUN chmod a+x /start_datahub_actions.sh

# Add other default configurations into this! 
COPY ./recipes/executor.yaml /etc/datahub/actions/

RUN addgroup --system datahub && adduser --system datahub --ingroup datahub
RUN chown datahub /etc/datahub


FROM ${APP_ENV}-install as final
USER datahub
CMD dockerize -wait http://$GMS_HOST:$GMS_PORT/health -timeout 240s /start_datahub_actions.sh

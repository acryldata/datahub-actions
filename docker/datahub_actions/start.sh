#!/bin/bash

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

# Deploy System Actions
system_config_files=""
for file in /etc/datahub/system/actions/*
do
    system_config_files+="-c ${file} "
done
datahub_actions actions -c $system_config_files > /tmp/datahub/logs/system/actions/system/actions.out

# Deploy User Actions
user_config_files=""
for file in /etc/datahub/actions/*
do
    user_config_files+="-c ${file} "
done
datahub_actions actions -c $user_config_files > /tmp/datahub/logs/actions/actions.out
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains a PostHog Hook
which allows you to connect to your PostHog account,
retrieve data from it or write to that file.

NOTE:   this hook also relies on the PostHog python library package:
        https://github.com/posthog/posthog-python
"""

import posthog

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class PostHogHook(BaseHook):
    """
    Create new connection to PostHog
    and allows you to pull data out of PostHog or write to it.

    You can then use that file with other
    Airflow operators to move the data around or interact with PostHog.

    :param posthog_conn_id: the name of the connection that has the parameters
        we need to connect to PostHog. The connection should be type `json` and include a
        write_key security token in the `Extras` field.
    :param posthog_debug_mode: Determines whether PostHog should run in debug mode.
        Defaults to False

    .. note::
        You must include a JSON structure in the `Extras` field.
        We need a user's security token to connect to PostHog.
        So we define it in the `Extras` field as:
        `{"write_key":"YOUR_SECURITY_TOKEN"}`
    """

    conn_name_attr = 'posthog_conn_id'
    default_conn_name = 'posthog_default'
    conn_type = 'posthog'
    hook_name = 'PostHog'

    def __init__(
        self, posthog_conn_id: str = 'posthog_default', posthog_debug_mode: bool = False, *args, **kwargs
    ) -> None:
        super().__init__()
        self.posthog_conn_id = posthog_conn_id
        self.posthog_debug_mode = posthog_debug_mode
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(self.posthog_conn_id)
        self.extras = self.connection.extra_dejson
        self.project_api_key = self.extras.get('project_api_key')
        if self.project_api_key is None:
            raise AirflowException('No PostHog write key provided')

    def get_conn(self) -> posthog:
        self.log.info('Setting write key for PostHog connection')
        posthog.debug = self.posthog_debug_mode
        if self.posthog_debug_mode:
            self.log.info('Setting PostHog connection to debug mode')
        posthog.on_error = self.on_error
        posthog.project_api_key = self.project_api_key
        return posthog

    def on_error(self, error: str, items: str) -> None:
        """Handles error callbacks when using PostHog with posthog_debug_mode set to True"""
        self.log.error('Encountered PostHog error: %s with items: %s', error, items)
        raise AirflowException(f'PostHog error: {error}')

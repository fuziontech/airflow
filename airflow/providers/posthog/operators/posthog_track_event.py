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
from typing import TYPE_CHECKING, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.posthog.hooks.posthog import PostHogHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PostHogTrackEventOperator(BaseOperator):
    """
    Send Track Event to PostHog for a specified user_id and event

    :param user_id: The ID for this user in your database. (templated)
    :param event: The name of the event you're tracking. (templated)
    :param properties: A dictionary of properties for the event. (templated)
    :param posthog_conn_id: The connection ID to use when connecting to PostHog.
    :param posthog_debug_mode: Determines whether PostHog should run in debug mode.
        Defaults to False
    """

    template_fields: Sequence[str] = ('user_id', 'event', 'properties')
    ui_color = '#ffd700'

    def __init__(
        self,
        *,
        user_id: str,
        event: str,
        properties: Optional[dict] = None,
        posthog_conn_id: str = 'posthog_default',
        posthog_debug_mode: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.user_id = user_id
        self.event = event
        properties = properties or {}
        self.properties = properties
        self.posthog_debug_mode = posthog_debug_mode
        self.posthog_conn_id = posthog_conn_id

    def execute(self, context: 'Context') -> None:
        hook = PostHogHook(posthog_conn_id=self.posthog_conn_id, posthog_debug_mode=self.posthog_debug_mode)

        self.log.info(
            'Sending track event (%s) for user id: %s with properties: %s',
            self.event,
            self.user_id,
            self.properties,
        )

        hook.capture(user_id=self.user_id, event=self.event, properties=self.properties)  # type: ignore

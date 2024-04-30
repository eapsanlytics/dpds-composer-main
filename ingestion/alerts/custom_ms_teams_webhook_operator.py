# -*- coding: utf-8 -*-
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

from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.decorators import apply_defaults
from ingestion.alerts.custom_ms_teams_webhook_hook import CustomMSTeamsWebhookHook
import logging


class CustomMSTeamsWebhookOperator(HttpOperator):
    """
    This operator allows you to post messages to MS Teams using the Incoming Webhooks connector.

    :param http_conn_id: The connection ID that has the MS Teams webhook URL.
    :type http_conn_id: str
    :param webhook_token: The MS Teams webhook token.
    :type webhook_token: str
    :param message: The message you want to send on MS Teams.
    :type message: str
    :param subtitle: The subtitle of the message to send.
    :type subtitle: str
    :param button_text: The text of the action button.
    :type button_text: str
    :param button_url: The URL for the action button click.
    :type button_url: str
    :param theme_color: The hex code of the card theme, without the #.
    :type theme_color: str
    :param proxy: The proxy to use when making the webhook request.
    :type proxy: str
    """

    template_fields = (
        "message",
        "subtitle",
    )

    @apply_defaults
    def __init__(
        self,
        http_conn_id=None,
        webhook_token=None,
        message="",
        subtitle="",
        proxy=None,
        context=None,
        *args,
        **kwargs
    ):

        super(CustomMSTeamsWebhookOperator, self).__init__(
            endpoint=webhook_token, *args, **kwargs
        )
        self.http_conn_id = http_conn_id
        self.webhook_token = webhook_token
        self.message = message
        self.subtitle = subtitle
        self.proxy = proxy
        self.context = context

    @property
    def hook(self) -> CustomMSTeamsWebhookHook:
        return CustomMSTeamsWebhookHook(
            self.http_conn_id,
            self.webhook_token,
            self.message,
            self.subtitle,
            self.proxy,
            self.context,
        )

    def execute(self, context):
        """
        Call the webhook with the required parameters
        """
        self.context = context
        self.hook.execute()
        logging.info("Webhook request sent to MS Teams")

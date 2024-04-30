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
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException


class CustomMSTeamsWebhookHook(HttpHook):
    """
    This hook allows you to post messages to MS Teams using the Incoming Webhook connector.

    Takes both MS Teams webhook token directly and connection that has MS Teams webhook token.
    If both supplied, the webhook token will be appended to the host in the connection.

    :param http_conn_id: Connection ID that has the MS Teams webhook URL
    :type http_conn_id: str
    :param webhook_token: MS Teams webhook token
    :type webhook_token: str
    :param message: Message to be sent to MS Teams
    :type message: str
    :param subtitle: Subtitle for the message
    :type subtitle: str
    :param proxy: Proxy to use when making the webhook request
    :type proxy: str
    :param context: Context information for the message
    :type context: dict
    """

    def __init__(
        self,
        http_conn_id=None,
        webhook_token=None,
        message="",
        subtitle="",
        proxy=None,
        context=None,
        *args,
        **kwargs,
    ):
        super(CustomMSTeamsWebhookHook, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = self.get_token(webhook_token, http_conn_id)
        self.message = message
        self.subtitle = subtitle
        self.proxy = proxy
        self.context = context

    def get_proxy(self, http_conn_id):
        conn = self.get_connection(http_conn_id)
        extra = conn.extra_dejson
        print(extra)
        return extra.get("proxy", "")

    def get_token(self, token, http_conn_id):
        """
        Given either a manually set token or a conn_id, return the webhook_token to use
        :param token: The manually provided token
        :param conn_id: The conn_id provided
        :return: webhook_token (str) to use
        """
        if token:
            return token
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            return extra.get("webhook_token", "")
        else:
            raise AirflowException(
                "Cannot get URL: No valid MS Teams " "webhook URL nor conn_id supplied"
            )

    def build_message(self):
        """
        Builds a message card in JSON format based on the state of the task instance.

        Returns:
            str: The message card in JSON format.
        """
        dag_id = self.context.get("task_instance").dag_id
        task_id = self.context.get("task_instance").task_id
        execution_date = self.context.get("execution_date")
        try_number = self.context.get("task_instance").try_number
        max_tries = self.context.get("task_instance").max_tries
        log_url = self.context.get("task_instance").log_url
        state = self.context.get("task_instance").state
        print(f"state = {state}")
        if state == "success" or state == "running":
            summary = f"{dag_id} completed successfully"
            title = f"**`{dag_id}` completed successfully**"
            theme_color = "008000"
            cardjson = """
                {{
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "markdown": true,
                    "summary": "{summary}",
                    "title": "Cloud Composer Alert",
                    "themeColor": "{theme_color}",
                    "sections": [
                        {{
                            "activityTitle": "{title}"
                        }},
                        {{
                            "text": "{message}"
                        }}
                    ],
                    "potentialAction": [
                        {{
                            "@type": "OpenUri",
                            "name": "View Log",
                            "targets": [
                                {{
                                    "os": "default",
                                    "uri": "{log_url}"
                                }}
                            ]
                        }}
                    ]
                }}"""

            cardjson2 = cardjson.format(
                theme_color=theme_color,
                summary=summary,
                dag_id=dag_id,
                log_url=log_url,
                state=state,
                message=self.message,
                title=title,
            )
        elif state == "failed" or state == "up_for_retry":
            dag_id = self.context.get("task_instance").dag_id
            task_id = self.context.get("task_instance").task_id
            execution_date = self.context.get("execution_date")
            try_number = self.context.get("task_instance").try_number
            max_tries = self.context.get("task_instance").max_tries
            log_url = self.context.get("task_instance").log_url
            state = self.context.get("task_instance").state
            print(f"state = {state}")
            if state == "failed":
                summary = f"Failure in {dag_id} - {task_id} task"
                title = f"**`{dag_id}` - `{task_id}` failed**"
                theme_color = "FF0000"
            elif state == "up_for_retry":
                summary = f"Retry in {dag_id} - {task_id} task"
                title = f"**`{dag_id}` - `{task_id}` retrying**"
                theme_color = "F4D03F"

            cardjson = """
            {{
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "markdown": true,
                "summary": "{summary}",
                "title": "Cloud Composer Alert",
                "themeColor": "{theme_color}",
                "sections": [
                    {{
                        "activityTitle": "{title}"
                    }},
                    {{
                        "facts": [
                            {{
                                "name": "Task ID:",
                                "value": "`{task_id}`"
                            }},
                            {{
                                "name": "Execution Date:",
                                "value": "`{execution_date}`"
                            }},
                            {{
                                "name": "Task State:",
                                "value": "`{state}`"
                            }},
                            {{
                                "name": "Try #:",
                                "value": "`{try_number}`"
                            }},
                            {{
                                "name": "Max Tries:",
                                "value": "`{max_tries}`"
                            }}
                        ]
                    }},
                    {{
                    }}
                ],
                "potentialAction": [
                    {{
                        "@type": "OpenUri",
                        "name": "View Log",
                        "targets": [
                            {{
                                "os": "default",
                                "uri": "{log_url}"
                            }}
                        ]
                    }}
                ]
            }}"""

            cardjson2 = cardjson.format(
                theme_color=theme_color,
                summary=summary,
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date,
                try_number=try_number,
                max_tries=max_tries,
                log_url=log_url,
                state=state,
                title=title,
            )
        print(cardjson2)
        return cardjson2

    def execute(self):
        """
        Remote Popen (actually execute the webhook call)

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        proxies = {}
        proxy_url = self.get_proxy(self.http_conn_id)
        print("Proxy is : " + proxy_url)
        if len(proxy_url) > 5:
            proxies = {"https": proxy_url}
        self.run(
            endpoint=self.webhook_token,
            data=self.build_message(),
            headers={"Content-type": "application/json"},
            extra_options={"proxies": proxies},
        )

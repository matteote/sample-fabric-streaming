# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# This function listens on an Eventhub and sends events back to the emulator
# via an Eventstream

import azure.functions as func
import logging
import json
import os

from azure.eventhub import EventData, EventHubProducerClient

app = func.FunctionApp()


@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="reflex_pa_trigger",
    connection="SOURCE_EVENT_HUB_CONNECTION_STRING",
)
def eventhub_trigger(azeventhub: func.EventHubEvent):
    source_event_data = json.loads(azeventhub.get_body().decode("utf-8"))

    logging.info("Python EventHub trigger processed an event: %s", source_event_data)

    if "message" in source_event_data:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=os.environ["TARGET_EVENT_HUB_CONNECTION_STRING"]
        )

        producer.send_event(EventData(json.dumps({"message": source_event_data["message"]})))

        producer.close()

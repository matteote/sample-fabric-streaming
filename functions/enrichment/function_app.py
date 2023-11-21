# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import azure.functions as func
import logging
import json
import os

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.eventhub import EventData, EventHubProducerClient

app = func.FunctionApp()


@app.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name="",
    connection="SOURCE_EVENT_HUB_CONNECTION_STRING",
)
def enrichment(azeventhub: func.EventHubEvent):
    event_data = json.loads(azeventhub.get_body().decode("utf-8"))

    event_data = flatten_characteristics(event_data)

    match event_data["Device"]["Type"]:
        case "Meter":
            event_data = add_meter_data(event_data)

        case "ElectricVehicle":
            event_data = add_ev_data(event_data)

    send_data(event_data)


def add_meter_data(event_data):
    site_name, customer_id, threshold = get_kql_row(f'GetMeterData(guid({event_data["Device"]["Id"]}))')
    event_data["site_name"] = site_name
    event_data["customer_id"] = customer_id

    if "power_consumption_wh" in event_data["Characteristics"]:
        event_data["Characteristics"]["delta_power_consumption_wh"] = (
            threshold - event_data["Characteristics"]["power_consumption_wh"]
        )

    return event_data


def add_ev_data(event_data):
    site_name, customer_id, battery_max = get_kql_row(f'GetEvData(guid({event_data["Device"]["Id"]}))')
    event_data["site_name"] = site_name
    event_data["customer_id"] = customer_id

    if "ev_battery_level" in event_data["Characteristics"]:
        event_data["Characteristics"]["delta_ev_battery_level"] = (
            battery_max - event_data["Characteristics"]["ev_battery_level"]
        )

    return event_data


def flatten_characteristics(event_data):
    event_data["Characteristics"] = dict(flatten_field(f) for f in event_data["Characteristics"])
    return event_data


def flatten_field(field):
    match field["ValueType"]:
        case "decimal":
            value = float(field["Value"])
        case "boolean":
            value = 1 if field["Value"].lower()=='true' else 0
        case _:
            value = field["Value"]

    return (field["Name"], value)


def send_data(event_data):
    logging.info(event_data)

    producer = EventHubProducerClient.from_connection_string(
        conn_str=os.environ["TARGET_EVENT_HUB_CONNECTION_STRING"]
    )
    producer.send_event(EventData(json.dumps(event_data)))

    producer.close()


def get_kql_row(query):
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        os.environ["KQL_URI"],
        os.environ["KQL_CLIENT_ID"],
        os.environ["KQL_CLIENT_SECRET"],
        os.environ["KQL_TENANT_ID"],
    )

    client = KustoClient(kcsb)
    response = client.execute("kqldb", query)

    for row in response.primary_results[0]:
        ret = row
        break

    client.close()
    return ret

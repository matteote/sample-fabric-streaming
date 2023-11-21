# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# This functions enriches events between two Eventstreams:
# - It adds customer ID and site name fields
# - Transforms the Characteristics array into a dictionary for consumption by the Reflex
# - Adds new metrics based on battery level and power consumption computing the delta
#   with the respective threshold values in KQL

# Expected parameters:
# SOURCE_EVENT_HUB_CONNECTION_STRING: Connection string of the eventstream the function received events from.
# TARGET_EVENT_HUB_CONNECTION_STRING: Connection string of the eventstream the function sends enriched events to.
# KQL_URI: URI of the KQL endpoint
# KQL_DB: Name of the KQL database
# KQL_CLIENT_ID: App ID of the application authorized to connect to KQL
# KQL_CLIENT_SECRET: App secret
# KQL_TENANT_ID: Entra ID (Azure AD) tenant ID

# Note: to authorize a registered app in KQL you must run a KQL management command as follows:
#
#     .add database mydatabase admins ('aadapp=APPLICATION_ID;TENANT_ID')
#
# where APPLICATION_ID must be replaced by the application ID of the app and TENANT_ID must be replaced by the
# Entra ID (Azure AD) tenant ID

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

    # Transform the array in Characteristics into a dictionary.
    event_data = flatten_characteristics(event_data)

    # Add device type-specific information
    match event_data["Device"]["Type"]:
        case "Meter":
            event_data = add_meter_data(event_data)

        case "ElectricVehicle":
            event_data = add_ev_data(event_data)

    send_data(event_data)


# Add site name, customer ID and delta power consumption to a meter device payload
def add_meter_data(event_data):
    site_name, customer_id, threshold = get_kql_row(
        f'GetMeterData(guid({event_data["Device"]["Id"]}))'
    )
    event_data["site_name"] = site_name
    event_data["customer_id"] = customer_id

    if "power_consumption_wh" in event_data["Characteristics"]:
        event_data["Characteristics"]["delta_power_consumption_wh"] = (
            threshold - event_data["Characteristics"]["power_consumption_wh"]
        )

    return event_data


# Add site name, customer ID and delta battery level to an EV device payload
def add_ev_data(event_data):
    site_name, customer_id, battery_max = get_kql_row(
        f'GetEvData(guid({event_data["Device"]["Id"]}))'
    )
    event_data["site_name"] = site_name
    event_data["customer_id"] = customer_id

    if "ev_battery_level" in event_data["Characteristics"]:
        event_data["Characteristics"]["delta_ev_battery_level"] = (
            battery_max - event_data["Characteristics"]["ev_battery_level"]
        )

    return event_data


# Transform the array in Characteristics into a dictionary.
#
# Per example from:
#    [
#        {
#            "Name": "ev_plugged_in",
#            "Value": "False",
#            "ValueType": "boolean"
#        },
#        {
#            "Name": "ev_battery_level",
#            "Value": "50",
#            "ValueType": "decimal"
#        },
#        {
#            "Name": "ev_charging",
#            "Value": "False",
#            "ValueType": "boolean"
#        }
#    ]
# To:
#    {
#        "ev_plugged_in": 0,
#        "ev_battery_level": 50,
#        "ev_charging": 0
#    }
def flatten_characteristics(event_data):
    event_data["Characteristics"] = dict(flatten_field(f) for f in event_data["Characteristics"])
    return event_data


# Turns Characteristics fields into tuples (name, value)
# The value is converted based on ValueType
#
# Note: booleans are turned into 1 or 0 (true or false, respectively) for
# easier filtering in the Reflex
def flatten_field(field):
    match field["ValueType"]:
        case "decimal":
            value = float(field["Value"])
        case "boolean":
            value = 1 if field["Value"].lower() == "true" else 0
        case _:
            value = field["Value"]

    return (field["Name"], value)


# Send an event to Eventstream
def send_data(event_data):
    logging.info(event_data)

    producer = EventHubProducerClient.from_connection_string(
        conn_str=os.environ["TARGET_EVENT_HUB_CONNECTION_STRING"]
    )
    producer.send_event(EventData(json.dumps(event_data)))

    producer.close()


# Retrieve the first row from a KQL query.
def get_kql_row(query):
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        os.environ["KQL_URI"],
        os.environ["KQL_CLIENT_ID"],
        os.environ["KQL_CLIENT_SECRET"],
        os.environ["KQL_TENANT_ID"],
    )

    client = KustoClient(kcsb)
    response = client.execute(os.environ["KQL_DB"], query)

    for row in response.primary_results[0]:
        ret = row
        break

    client.close()
    return ret

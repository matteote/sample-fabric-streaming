# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Run the script without parameters for help.
# The script can be configured via file. Use emulator.ini.example as a reference.

import asyncio
import datetime
import json
import uuid

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient, EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from configargparse import ArgParser


class Emulator:
    def __init__(self, args):
        self.args = args
        self._power_consumption_wh = 1000
        self.ev_charging = False
        self.ev_plugged_in = False
        self.ev_battery_level = 50

        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.args.target_event_hub_connection_string,
            eventhub_name=self.args.target_event_hub_name,
        )

    def power_consumption_wh(self):
        return self._power_consumption_wh + (1000 if self.ev_charging else 0)

    # Send telemetry of meter and EV every 60 seconds
    async def send(self):
        while True:
            data = {
                "EventInstanceId": str(uuid.uuid4()),
                "EventName": "SensorReadingEvent",
                "EventTime": datetime.datetime.utcnow().isoformat(),
                "Device": {
                    "Id": "de286df4-71ea-427e-8519-104830ae1559",
                    "Type": "Meter",
                },
                "Characteristics": [
                    {
                        "Name": "power_consumption_wh",
                        "Value": str(self.power_consumption_wh()),
                        "ValueType": "decimal",
                    }
                ],
            }
            await self.send_event(data)

            data = {
                "EventInstanceId": str(uuid.uuid4()),
                "EventName": "SensorReadingEvent",
                "EventTime": datetime.datetime.utcnow().isoformat(),
                "Device": {
                    "Id": "85fd20a9-c85c-4dc0-a6c9-2e20d5df1de2",
                    "Type": "ElectricVehicle",
                },
                "Characteristics": [
                    {
                        "Name": "ev_plugged_in",
                        "Value": str(self.ev_plugged_in),
                        "ValueType": "boolean",
                    },
                    {
                        "Name": "ev_battery_level",
                        "Value": str(self.ev_battery_level),
                        "ValueType": "decimal",
                    },
                    {
                        "Name": "ev_charging",
                        "Value": str(self.ev_charging),
                        "ValueType": "boolean",
                    },
                ],
            }
            await self.send_event(data)

            await asyncio.sleep(60)

    # Helper to send events over eventstream
    async def send_event(self, data):
        print(f"Sending data:{data}")

        async with self.producer:
            await self.producer.send_event(EventData(json.dumps(data)))

    # Responds to events received from eventstream
    async def on_event(self, partition_context, event):
        try:
            body = event.body_as_str(encoding="UTF-8")
            message = json.loads(body)
            print(f"Received: {message}")

            # Turn off the EV charging
            if message.get("message", None) == "TurnEvChargingOff":
                print("EV charging off")
                self.ev_charging = False
                self.print_status()

            # Turn on the EV charging
            if message.get("message", None) == "TurnEvChargingOn":
                print("EV charging on")
                self.ev_charging = True
                self.print_status()

        except json.decoder.JSONDecodeError:
            print(f"Failed deserializing {body}")

        await partition_context.update_checkpoint(event)

    # Listen on evenstream
    async def receive(self):
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            self.args.storage_connection_string, self.args.storage_container
        )

        client = EventHubConsumerClient.from_connection_string(
            self.args.source_event_hub_connection_string,
            consumer_group="$Default",
            eventhub_name=self.args.source_event_hub_name,
            checkpoint_store=checkpoint_store,
        )

        async with client:
            await client.receive(on_event=self.on_event, starting_position="-1")

    # Listen on keyboard
    # User must type a letter and press enter
    async def user_input(self):
        while True:
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, input)
            if content == "a":  # Increase power consumption
                self._power_consumption_wh += 100
            elif content == "z":  # Decrease power consumption
                if self._power_consumption_wh > 100:
                    self._power_consumption_wh -= 100
                else:
                    self._power_consumption_wh = 0
            elif content == "s":  # Increase battery level
                if self.ev_battery_level <= 90:
                    self.ev_battery_level += 10
                else:
                    self.ev_battery_level = 100
            elif content == "x":  # Decrease battery level
                if self.ev_battery_level > 10:
                    self.ev_battery_level -= 10
                else:
                    self.ev_battery_level = 0
            elif content == "e":  # Plug/unplug EV
                self.ev_plugged_in = not self.ev_plugged_in
                if not self.ev_plugged_in:
                    self.ev_charging = 0
            self.print_status()

    # Print the status of the emulator
    def print_status(self):
        print(f"Power consumption: {self.power_consumption_wh()}")
        print(f"EV charging: {self.ev_charging}")
        print(f"EV plugged in: {self.ev_plugged_in}")
        print(f"EV battery: {self.ev_battery_level}")

    # Main loop
    async def run(self):
        tasks = [self.send(), self.receive(), self.user_input()]
        await asyncio.gather(*tasks)


p = ArgParser(default_config_files=["emulator.ini"])
p.add("-c", "--config-file", required=False, is_config_file=True, help="Config file path")
p.add(
    "--source-event-hub-connection-string",
    required=True,
    help="Connection string of the Eventstream the emulator receives commands from.",
)
p.add(
    "--source-event-hub-name",
    required=True,
    help="Item name of the Eventstream the emulator receives commands from.",
)
p.add(
    "--target-event-hub-connection-string",
    required=True,
    help="Connection string of the Eventstream the emulator sends telemetry to.",
)
p.add(
    "--target-event-hub-name",
    required=True,
    help="Item name of the Eventstream the emulator sends telemetry to.",
)
p.add(
    "--storage-connection-string",
    required=True,
    help="Connection string of the storage account for the blob checkpoint store.",
)
p.add("--storage-container", required=True, help="Container for the blob checkpoint store.")

args = p.parse_args()
emulator = Emulator(args)

asyncio.run(emulator.run())

# Sample: Fabric streaming with Eventstream, Data Activator and KQL

> [!NOTE]
> This repository contains sample code and designs developed for demonstration purposes;
> is it not meant to be production-ready.

## Content of the repo

- **emulator**: A sample python script that generates telemetry for two devices from a single site.
- **functions/enrichment**: Azure Function with Eventhub trigger that enriches event streamed over Eventstream
- **functions/trigger**: Azure Function with Eventhub trigger that listens on Eventhub and sends events back to the emulator via Eventream.
- **KQL Schema.sql**: Minimal KQL schema used for the sample. It contains telemety table, lookup tables for devices and site configuration, and two functions used during lookup by the enrichment function.

## Architecture

![Sample architecture](images/Sample%20architecture.drawio.svg)

## Reflex configuration

The Reflex is configured to consume telemetry with the Site name ad the key column.

There are three triggers.

Trigger to turn on charging when the EV is plugged in, the batter is lower than the desired level and the power consumption is below a defined threshold:

![Trigger turn_ev_charging_on](images/turn_ev_charging_on.png)

Trigger to turn off charging when the battery reaches the desired level:

![Trigger turn_ev_chargin_off_full](images/turn_ev_chargin_off_full.png)

Trigger to turn off charging when the power consumption exceeds a predefined threshold.

![Trigger turn_ev_charging_off_threhold](images/turn_ev_charging_off_threhold.png)

All triggers use the same custom action to send a message via the same Power Automate flow:

![Custom action](images/custom_action.png)

![Power Automate Flow](images/power_automate_flow.png)

The Power Automate Flow in turns sends the content of the event to an Eventhub that is consumed by the trigger functions to send events back to the emulator.

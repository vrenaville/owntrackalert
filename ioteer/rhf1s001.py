import paho.mqtt.client as mqtt
import json
import os
import logging
import signal
from datetime import datetime
from influxdb import InfluxDBClient
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(filename=".env2"))
SRC_MQTT_HOST = os.getenv("SRC_MQTT_HOST")
SRC_MQTT_USER = os.getenv("SRC_MQTT_USER")
SRC_MQTT_PASS = os.getenv("SRC_MQTT_PASS")
DST_INFLUX_HOST = os.getenv("DST_INFLUX_HOST")
DST_INFLUX_USER = os.getenv("DST_INFLUX_USER")
DST_INFLUX_PASS = os.getenv("DST_INFLUX_PASS")
DST_INFLUX_DB = os.getenv("DST_INFLUX_DB")

def on_connect_ttn(client, userdata, flags, rc):
    logging.info("connected to ttn %s - %s", SRC_MQTT_HOST, str(rc))
    client.subscribe("+/devices/+/up")

def on_log(client, userdata, level, buf):
    logging_level = mqtt.LOGGING_LEVEL[level]
    logging.log(logging_level, buf)
    logging.info("got a log message level %s: %s", level, str(buf))

# The callback for when a PUBLISH message is received from the server.
def on_message_ttn(client, userdata, msg):
    data = json.loads(msg.payload)
    logging.info("message from ttn received for %s - #%s", data["dev_id"], data["counter"])
    logging.info("received via gw %s", data["metadata"]["gateways"][0]["gtw_id"])

    # write to influxdb
    logging.info("writing data to influxdb")
    influxdb.write_points(
    [
        {
            "measurement": "risinghf",
            "tags": {
                "device": "rhf1s001",
            },
            "fields": {
                "battery": data["payload_fields"]["battery"],
                "hum": data["payload_fields"]["hum"],
                "temp": data["payload_fields"]["temp"],
                "counter": data["counter"],
                "rssi": data["metadata"]["gateways"][0]["rssi"],
            }
        }
    ] 
    )

    logging.info("data processing done")

def shutdown():
    logging.info("disconnecting from mqtt")
    client_ttn.disconnect()
    client_ttn.loop_stop()

def handleSIGTERM(signalNumber, frame):
    logging.info("got SIGTERM")
    shutdown()
    return

if __name__ == '__main__':

    signal.signal(signal.SIGTERM, handleSIGTERM)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %Z'
    )

    # Prepare InfluxDB
    influxdb = InfluxDBClient(
            host=DST_INFLUX_HOST,
            port=443,
            database=DST_INFLUX_DB,
            username=DST_INFLUX_USER,
            password=DST_INFLUX_PASS,
            ssl=True,
            verify_ssl=True,
    )

    # Prepare MQTT
    client_ttn = mqtt.Client()
    client_ttn.enable_logger()
    client_ttn.on_connect = on_connect_ttn
    client_ttn.on_message = on_message_ttn
    client_ttn.on_log = on_log
    client_ttn.username_pw_set(SRC_MQTT_USER,SRC_MQTT_PASS)
    client_ttn.tls_set()
    client_ttn.connect(SRC_MQTT_HOST, 8883, 60)

    try:
        # Connect to MQTT and react to messages
        client_ttn.loop_forever()
    except KeyboardInterrupt:
        client_ttn.disconnect()
        client_ttn.loop_stop()
        logging.info("tschuess")
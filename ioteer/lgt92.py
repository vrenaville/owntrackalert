import paho.mqtt.client as mqtt
import json
import os
import logging
import signal
import requests
from datetime import datetime
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())
SRC_MQTT_HOST = os.getenv("SRC_MQTT_HOST")
SRC_MQTT_USER = os.getenv("SRC_MQTT_USER")
SRC_MQTT_PASS = os.getenv("SRC_MQTT_PASS")
DST_MQTT_HOST = os.getenv("DST_MQTT_HOST")
DST_MQTT_USER = os.getenv("DST_MQTT_USER")
DST_MQTT_PASS = os.getenv("DST_MQTT_PASS")
HC_PING_URL = os.getenv("HC_PING_URL")
TRACEPING = os.getenv("TRACEPING")

VERSION = "v2.0"

OT_TOPIC="owntracks/alexandre/dragino"
BATTERY_TOPIC="envcontrol/dragino/dragino/battery"
OT_TID="dragino"
ALERT_FLAG = {}
def on_connect_ttn(client, userdata, flags, rc):
    logging.info("connected to ttn %s - %s", SRC_MQTT_HOST, str(rc))
    #client.subscribe("+/devices/+/up")
    #client.subscribe("#")
    client.subscribe("v3/+/devices/+/up")
def on_connect_ot(client, userdata, flags, rc):
    logging.info("connected to ot %s - %s", DST_MQTT_HOST, str(rc))

def on_publish_ot(client, userdata, rc):
    logging.info("published data to ot")

def on_log(client, userdata, level, buf):
    logging_level = mqtt.LOGGING_LEVEL[level]
    logging.log(logging_level, buf)
    #logging.info("got a log message level %s: %s", level, str(buf))
    if "PINGRESP" in str(buf):
        # report to https://healthchecks.io to tell that the connection is alive
        requests.get(HC_PING_URL)

def jsonping(data,OT_TID,event):
    gtw_id = data["uplink_message"]["rx_metadata"][0]["gateway_ids"]["gateway_id"]
    ot_data = json.dumps({
        "_type": "transition",
        "wtst": int(datetime.timestamp(datetime.now())),
        "lat": data["uplink_message"]["decoded_payload"]["Latitude"],
        "lon": data["uplink_message"]["decoded_payload"]["Longitude"],
        "tst": int(datetime.timestamp(datetime.now())),
        "acc": 0,
        "tid": OT_TID,
        "event": event,
        "desc": "Ping from %s received by %s" %(OT_TID, gtw_id),
        "t": "c",
    })
    return ot_data

def pingenten(data,OT_TID):
    return jsonping(data,OT_TID,"enter")

def pingleave(data,OT_TID):
   return jsonping(data,OT_TID,"leave")


# The callback for when a PUBLISH message is received from the server.
def on_message_ttn(client, userdata, msg):
    data = json.loads(msg.payload)
    logging.info("message from ttn received for %s", data["end_device_ids"]["dev_eui"])

    # retrieve info about gateway
    gtw_id = data["uplink_message"]["rx_metadata"][0]["gateway_ids"]["gateway_id"]
    try:
        gtw_info = requests.get("https://www.thethingsnetwork.org/gateway-data/gateway/"+gtw_id).json()
        logging.info("received via gw %s, %s, owned by %s",
            gtw_id,
            gtw_info[gtw_id]["description"],
            gtw_info[gtw_id]["owner"],
        )
    except:
        logging.info("received via gw %s", gtw_id)

    # max is 4 volts, 3 volts is considered empty
    batpercent = round((data["uplink_message"]["decoded_payload"]["BatV"] - 3) * 100)

    if data["uplink_message"]["decoded_payload"]["ALARM_status"] != "FALSE":
        ot_data = json.dumps({
            "_type": "transition",
            "wtst": int(datetime.timestamp(datetime.now())),
            "lat": data["uplink_message"]["decoded_payload"]["Latitude"],
            "lon": data["uplink_message"]["decoded_payload"]["Longitude"],
            "tst": int(datetime.timestamp(datetime.now())),
            "acc": 0,
            "tid": OT_TID,
            "event": "enter",
            "desc": "ALERT BUTTON PRESSED !!!!!",
            "t": "c",
        })
        ALERT_FLAG[OT_TID] = 1
        # publish to owntracks
        logging.info("publishing alert to owntracks via mqtt to topic %s", OT_TOPIC)
        client_ot.publish(OT_TOPIC, payload=ot_data, retain=True, qos=1)
        logging.info("Red button pushed!")
    else:
        if OT_TID in ALERT_FLAG and ALERT_FLAG[OT_TID] == 1:
            ot_data = json.dumps({
                "_type": "transition",
                "wtst": int(datetime.timestamp(datetime.now())),
                "lat": data["uplink_message"]["decoded_payload"]["Latitude"],
                "lon": data["uplink_message"]["decoded_payload"]["Longitude"],
                "tst": int(datetime.timestamp(datetime.now())),
                "acc": 0,
                "tid": OT_TID,
                "event": "leave",
                "desc": "ALERT BUTTON finished :)",
                "t": "c",
            })
            # publish to owntracks
            logging.info("publishing alert to owntracks via mqtt to topic %s", OT_TOPIC)
            client_ot.publish(OT_TOPIC, payload=ot_data, retain=True, qos=1)
            logging.info("Alert finished!")
            ALERT_FLAG[OT_TID] = 0
        else:
            ALERT_FLAG[OT_TID] = 0
    if TRACEPING == "1":
        logging.info("%s", data)
        logging.info("DEBUG: Ping from %s", OT_TID)
        logging.info("DEBUG: Received from %s", gtw_id)
        #client_ot.publish(OT_TOPIC, payload=pingenten(data,OT_TID), retain=True, qos=1)
        #client_ot.publish(OT_TOPIC, payload=pingleave(data,OT_TID), retain=True, qos=1)

    logging.info("Motion detection: %s", data["uplink_message"]["decoded_payload"]["MD"])
    logging.info("LED status for position: %s", data["uplink_message"]["decoded_payload"]["LON"])
    logging.info("Firmware version: %s", data["uplink_message"]["decoded_payload"]["FW"])

    got_fix = False
    if data["uplink_message"]["decoded_payload"]["Latitude"] == 0 or data["uplink_message"]["decoded_payload"]["Latitude"] == -1e-06 :
        logging.info("no GPS data (Latitude) present")
        # set GPS data to 0 for InfluxDB
    else:
        logging.info("GPS data (Latitude) present: lat %s, lon %s",
          data["uplink_message"]["decoded_payload"]["Latitude"],
          data["uplink_message"]["decoded_payload"]["Longitude"]
        )
        got_fix = True
        # transform received data into OwnTracks format
        ot_data = json.dumps({
            "_type": "location",
            "acc": 0,
            "alt": data["uplink_message"]["decoded_payload"]["Altitude"],
            "vac": 0,
            "vel": 0,
            "conn": 0,
            "lat": data["uplink_message"]["decoded_payload"]["Latitude"],
            "lon": data["uplink_message"]["decoded_payload"]["Longitude"],
            "batt": batpercent,
            "t": "p",
            "tid": OT_TID,
            "tst": int(datetime.timestamp(datetime.now())),
            "conn": "m",
        })

        # publish to owntracks
        logging.info("publishing data to owntracks via mqtt to topic %s", OT_TOPIC)
        client_ot.publish(OT_TOPIC, payload=ot_data, retain=True, qos=1)
    env_data = json.dumps({
        "date": int(datetime.timestamp(datetime.now())),
        "user": data["end_device_ids"]["dev_eui"],
        "battery": batpercent,
    })
    logging.info("publishing data to battery via mqtt to topic %s", BATTERY_TOPIC)
    client_ot.publish(BATTERY_TOPIC, payload=env_data, retain=True, qos=1)


    logging.info("data processing done")

def shutdown():
    logging.info("disconnecting from mqtt")
    client_ot.disconnect()
    client_ot.loop_stop()
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

    logging.info("Starting ioteer lgt92. "+VERSION)

    # Prepare MQTT for The Things Network
    client_ttn = mqtt.Client()
    client_ttn.enable_logger()
    client_ttn.on_connect = on_connect_ttn
    client_ttn.on_message = on_message_ttn
    client_ttn.on_log = on_log
    client_ttn.username_pw_set(SRC_MQTT_USER,SRC_MQTT_PASS)
    client_ttn.tls_set()
    client_ttn.connect(SRC_MQTT_HOST, 8883, 60)

    # Prepare MQTT for OwnTracks
    ot_lwt = json.dumps({
        "_type": "lwt",
        "tst": int(datetime.timestamp(datetime.now())),
    })
    client_ot = mqtt.Client()
    client_ot.enable_logger()
    client_ot.on_connect = on_connect_ot
    client_ot.on_publish = on_publish_ot
    client_ot.on_log = on_log
    client_ot.username_pw_set(DST_MQTT_USER,DST_MQTT_PASS)
    client_ot.tls_set()
    client_ot.will_set(OT_TOPIC, payload=ot_lwt, qos=1, retain=True)
    client_ot.connect(DST_MQTT_HOST, 8883, 60)

    try:
        # Connect to MQTT and react to messages
        logging.info("Start waiting messages")
        client_ot.loop_start()
        client_ttn.loop_forever()
    except KeyboardInterrupt:
        shutdown()
        logging.info("Shutting down")


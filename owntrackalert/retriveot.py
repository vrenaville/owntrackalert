import paho.mqtt.client as mqtt
import json
import os
import logging
import signal
import requests
from datetime import datetime
from datetime import timedelta

from dotenv import find_dotenv, load_dotenv
import sqlite3
from geo_position import GeoPositionAlerting

load_dotenv(find_dotenv())
MQTT_HOST = os.getenv("DST_MQTT_HOST")
MQTT_USER = os.getenv("DST_MQTT_USER")
MQTT_PASS = os.getenv("DST_MQTT_PASS")

VERSION = "v1.0"
CON = False
OT_TID="dragino"
OT_TOPIC="owntracks/recorder/alert"
USER_LAST_SEEN={}
USER_ALARM_LEVEL={}

def on_connect_ot(client, userdata, flags, rc):
    logging.info("connected to ot %s - %s", MQTT_HOST, str(rc))
    client.subscribe("owntracks/#")


def on_publish_ot(client, userdata, rc):
    logging.info("published data to ot")

def on_log(client, userdata, level, buf):
    logging_level = mqtt.LOGGING_LEVEL[level]
    logging.log(logging_level, buf)

def CreateUpdateUser(cur, tid):
    cur.execute("SELECT id FROM users where name =?", (tid,))
    
    rows = cur.fetchall()
    if rows:
        return rows[0][0]
    else:
        cur.execute("INSERT INTO users (name) VALUES (?)", (tid,))
        return cur.lastrowid

def getpreviousposition(cur,user_id):
    sql_query = """SELECT longitude,latitude from points
        where userid = ? and timestamp BETWEEN ? AND ? ORDER BY ID"""
    query = cur.execute(
        sql_query,
        (user_id,int(datetime.timestamp(datetime.now() - timedelta(0,600))),int(datetime.timestamp(datetime.now()))))

    return query.fetchall()

def getwaypoints(cur):
    sql_query = """SELECT longitude,latitude, radius from waypoints"""
    query = cur.execute(
        sql_query)
    return query.fetchall()


def jsonping(data,event):
    ot_data = json.dumps({
        "_type": "transition",
        "wtst": int(datetime.timestamp(datetime.now())),
        "lat": data["lat"],
        "lon": data["lon"],
        "tst": int(datetime.timestamp(datetime.now())),
        "acc": 0,
        "tid": data["tid"],
        "event": event,
        "desc": "No move from %s since 10 minutes" %(data["tid"],),
        "t": "c",
    })
    return ot_data

def pingenten(data):
    return jsonping(data,"enter")

def pingleave(data):
   return jsonping(data,"leave")


# The callback for when a PUBLISH message is received from the server.
def on_message_ot(client, userdata, msg):
    data = json.loads(msg.payload)
    if data['_type'] == 'location':
        logging.info("point from ot received for %s", data["tid"])
        cur = CON.cursor()
        user_id = CreateUpdateUser(cur, data["tid"])
        sql_record = {
        "accuracy": data["acc"],
        "altitude":data["alt"],
        "battery":data["batt"],
        "latitude":data["lat"],
        "longitude":data["lon"],
        "trackerid":data["tid"],
        "timestamp":data["tst"],
        "verticalaccuracy":data["vac"],
        "velocity":data["vel"],
        "connection":data["conn"],
        "userid":user_id,
        
        }
        columns = ', '.join(sql_record.keys())
        placeholders = ', '.join('?' * len(sql_record))
        sql = 'INSERT INTO points ({}) VALUES ({})'.format(columns, placeholders)
        values = [int(x) if isinstance(x, bool) else x for x in sql_record.values()]
        cur.execute(sql, values)
        CON.commit()
        # manage alarm
        lasteen = USER_LAST_SEEN.get(user_id,False)
        levelalarm = USER_ALARM_LEVEL.get(user_id,0)
        geocheck = GeoPositionAlerting(user_id=user_id,lastseen=lasteen,alertinglevel=levelalarm,radius=50)
        check_needed, check_date = geocheck.needcheck()
        USER_LAST_SEEN[user_id] = check_date
        if check_needed or levelalarm != 0:
            pointlist = getpreviousposition(cur,user_id)
            needalarm, levelalarm=geocheck.checkraisealarm(pointlist,[data["lon"],data["lat"]])
            if needalarm:
                client_ot.publish(OT_TOPIC,payload=pingenten(data), retain=True, qos=1)
                USER_ALARM_LEVEL[user_id] = levelalarm
            else:
                client_ot.publish(OT_TOPIC,payload=pingleave(data), retain=True, qos=1)
                USER_ALARM_LEVEL[user_id] = 0
                USER_LAST_SEEN[user_id] = False

            logging.info("DEBUG : ")
 
        logging.info("data processing done")
    elif data['_type'] == 'lwt':
        logging.info("Lost connection")
    elif data['_type'] == 'waypoint':
        logging.info("Waypoint")
        sql_record = {
            "longitude": data["lon"],
            "latitude": data["lat"],
            "name": data["desc"],
            "radius": data["rad"],
            "comment": "",
        }
        cur = CON.cursor()

        columns = ', '.join(sql_record.keys())
        placeholders = ', '.join('?' * len(sql_record))
        sql = 'INSERT INTO waypoints ({}) VALUES ({})'.format(columns, placeholders)
        values = [int(x) if isinstance(x, bool) else x for x in sql_record.values()]
        cur.execute(sql, values)
        CON.commit()

def shutdown():
    logging.info("disconnecting from mqtt")
    client_ot.disconnect()
    client_ot.loop_stop()
    CON.close()

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
    CON = sqlite3.connect('/tmp/owntrackstore')
    client_ot = mqtt.Client()
    client_ot.enable_logger()
    client_ot.on_connect = on_connect_ot
    client_ot.on_message = on_message_ot
    client_ot.on_publish = on_publish_ot
    client_ot.on_log = on_log
    client_ot.username_pw_set(MQTT_USER,MQTT_PASS)
    client_ot.tls_set()
    #client_ot.will_set(OT_TOPIC, payload=ot_lwt, qos=1, retain=True)
    client_ot.connect(MQTT_HOST, 8883, 60)
    
    try:
        # Connect to MQTT and react to messages
        logging.info("Start waiting messages")
        client_ot.loop_forever()
    except KeyboardInterrupt:
        shutdown()
        logging.info("Shutting down")


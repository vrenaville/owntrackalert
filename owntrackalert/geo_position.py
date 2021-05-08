import json
import os
import logging
from datetime import datetime
import sqlite3
from geopy.distance import geodesic

class GeoPositionAlerting:
    lastseen = False
    alertinglevel = 0
    autorisedradius = 0
    sqlcursor = False
    checkingrate = 0
    user_id = 0

    def __init__(self, cursor, user_id, lastseen, alertinglevel, radius):
        self.lastseen = lastseen
        self.alertinglevel = alertinglevel
        self.autorisedradius = radius
        self.sqlcursor = cursor
        self.user_id = user_id
    
    def needcheck(self):
        check_date = False
        check_needed = False
        if not self.lastseen:
            check_date = datetime.now()
            check_needed = False
        elif self.lastseen >= (datetime.now() + datetime.timedelta(0,60)):
            check_date = datetime.now()
            check_needed = False
        elif self.lastseen >= (datetime.now() + datetime.timedelta(0,10)):
            check_date = datetime.now()
            check_needed = True
        else:
            check_needed = False
            check_date = self.lastseen
        return check_needed, check_date

    def getpreviousposition(self):
        sql_query = """SELECT longitude,latitude from points
         where userid = ? and timestamp BETWEEN ? AND ? ORDER BY ID"""
        query = self.sqlcursor.execute(
            sql_query,
            (self.user_id,datetime.now(),datetime.timedelta(0,10)))
        return query

    def checkraisealarm(self,points,currentpoint):
        if not points:
            return False
        # Get oldest point:
        old_longitude = points[0][0]
        old_latitude = points[0][1]
        olddest_point = (old_longitude, old_latitude)
        newpoint = (current_point[longitude], current_point[latutde])
        distance = geodesic(olddest_point, newpoint).meters
        if distance < self.autorisedradius:
            return True
        else:
            return False

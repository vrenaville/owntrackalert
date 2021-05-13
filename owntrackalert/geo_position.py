import json
import os
import logging
from datetime import datetime
from datetime import timedelta

import sqlite3
from geopy.distance import geodesic

class GeoPositionAlerting:
    lastseen = False
    alertinglevel = 0
    autorisedradius = 0
    sqlcursor = False
    checkingrate = 0
    user_id = 0

    def __init__(self, user_id, lastseen, alertinglevel, radius):
        self.lastseen = lastseen
        self.alertinglevel = alertinglevel
        self.autorisedradius = radius
        self.user_id = user_id
    
    def needcheck(self):
        check_date = False
        check_needed = False
        if not self.lastseen:
            check_date = datetime.now()
            check_needed = False
        elif self.lastseen >= (datetime.now() + timedelta(0,60)):
            check_date = datetime.now()
            check_needed = False
        elif self.lastseen >= (datetime.now() + timedelta(0,10)):
            check_date = datetime.now()
            check_needed = True
        else:
            check_needed = False
            check_date = self.lastseen
        return check_needed, check_date

 
    def checkraisealarm(self,points,currentpoint):
        if not points:
            return False
        # Get oldest point:
        old_longitude = points[0][0]
        old_latitude = points[0][1]
        olddest_point = (old_longitude, old_latitude)
        newpoint = (currentpoint[0], currentpoint[1])
        distance = geodesic(olddest_point, newpoint).meters
        if distance < self.autorisedradius:
            return True
        else:
            return False

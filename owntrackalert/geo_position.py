import json
import os
import logging
from datetime import datetime
import sqlite3


class GeoPositionAlerting:
    lastseendict = {}
    alertinglevel = {}
    autorisedradius = 0
    sqlcursor = False
    checkingrate = 0

    def __init__(self, cursor, lastseendict, alertinglevel, radius):
        self.lastseendict = lastseendict
        self.alertinglevel = alertinglevel
        self.autorisedradius = radius
        self.sqlcursor = cursor
    
    def needcheck:



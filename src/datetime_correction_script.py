"""
OBSOLETE

I made a boneheaded datetime string encoding error, and then wrote several thousand trips to the database while that
error was live. So I now need to write a script (this one) which goes back through all of those entries and corrects
all of their dates to the right format.

That error has now been fixed in the latest version of `citibike_trips.py`.
"""


import citibike_trips
from datetime import datetime


db = citibike_trips.DataStore(uri="mongodb://localhost:27017")
for trip in db.iter_all():
    if trip['properties']['starttime'][:9] != '6/22/2016':
        trip['properties']['starttime'] = datetime.strptime(trip['properties']['starttime'], "%Y-%d-%m %H:%M:%S")\
            .strftime("%m/%d/%Y %H:%M:%S")\
            .lstrip('0')
        trip['properties']['stoptime'] = datetime.strptime(trip['properties']['stoptime'], "%Y-%d-%m %H:%M:%S")\
            .strftime("%m/%d/%Y %H:%M:%S")\
            .lstrip('0')
        db.replace_trip(trip['properties']['tripid'], trip)

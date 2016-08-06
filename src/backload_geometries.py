"""
OBSOLETE

Uses the pre pathing-factor-out data in the data store to pre-populate the paths for the post- (current) version.
"""

import citibike_trips
from tqdm import tqdm

trip_index = []
db = citibike_trips.DataStore(uri="mongodb://localhost:27017")
for entry in tqdm(db.client['citibike']['citibike-trips-old-dupe'].find({})):
    coords = entry['geometry']['coordinates']
    start_station = entry['properties']['start station id']
    end_station = entry['properties']['end station id']
    if (start_station, end_station) in trip_index or (end_station, start_station) in trip_index:
        pass
    else:
        db.client['citibike']['trip-geometries'].insert_one({
            'start station id': start_station,
            'end station id': end_station,
            'coordinates': entry['geometry']['coordinates']
        })
        trip_index.append((start_station, end_station))

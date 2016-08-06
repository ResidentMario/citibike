"""
OBSOLETE

Builds a sampler of complete bike-week paths based on the list of bike-weeks stored in the data store.

Following a project pivot, no longer works.
"""

from src.citibike_trips import DataStore


db = DataStore()
coords_list = []
for geojson_repr in db.select_all():
    for feature in geojson_repr['features']:
        coords_list.append(feature['geometry']['coordinates'])
with open('../data/part_1/bike_week_sampler.json', 'w') as f:
    f.write(str(coords_list))
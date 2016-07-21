"""
Builds a sampler of complete bike-week paths based on the list of bike-weeks stored in the data store.
"""

from citibike_trips import DataStore


db = DataStore()
coords_list = []
for geojson_repr in db.select_all():
    print(geojson_repr)
    print(geojson_repr.keys())
    for feature in geojson_repr['features']:
        coords_list.append(feature['geometry']['coordinates'])
with open('bike_week_sampler.json', 'w') as f:
    f.write(str(coords_list))
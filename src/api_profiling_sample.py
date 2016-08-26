"""
This script simulates running a request for all trips in bikes mode for the Penn Station Valet station,
a request which asks for 3376 trips total and took two minutes (!) to process in a benchmark run before this script
was generated.

This script extracts parts of `citibike_api` relevant to the request in question and runs them inline. It is meant to be
used with `vprof` (https://github.com/nvdv/vprof) or just cProfiler to determine hotspots which could be addressed in
order to reduce API runtime.

Note: the `citibike_api` code was copy-pasted directly into this file. This is not necessary for cProfile,
but *is* necessary for `vprof`.
"""

# from pymongo import MongoClient
# import json
#
# penn_station_valet_station_id = 3230
# mongo_uri = json.load(open("static/post_assets/citibike/mlab_instance_api_key.json"))['uri']
#
# # Code taken from `citibike_trips.DataStore.__init__`.
# client = MongoClient(uri)
# client.server_info()
# # If an index on (start station id, end station id) pairs have not already been created, create it.
# # This operation is idempotent, if the index already exists it does nothing.
# self.client['citibike']['trip-geometries'].create_index([('start station id', pymongo.ASCENDING),
#                                                          ('end station id', pymongo.ASCENDING)])

from citibike_trips import DataStore
import json

################
# RUNTIME CODE #
################

from flask import Response

mongo_uri = json.load(open("../credentials/mlab_instance_api_key.json"))['uri']
db = DataStore(mongo_uri)
tripset = db.get_station_bikeset(str(3230), 'outbound bike trip indices')
# Remove None trips---these correspond with trips that have not been populated in the database yet!
tripset = [trip for trip in tripset if trip is not None]
# jsonify(tripset) will not work because Flask disallows lists within arrays in top-level JSON, for security
# reasons. However the security issue in question seems to have been patched out long ago in all major browsers?
# Further reference: https://github.com/pallets/flask/issues/673;
# http://flask.pocoo.org/docs/0.11/security/#json-security
# return jsonify(tripset)
# json.dumps has no such qualms. It also handles the fact that the output is single-quoted strings, while JSON
# enforces double-quoted string (so you can't e.g. cast to a straight string using str(tripset)!).
response = Response(json.dumps(tripset), mimetype='application/json')



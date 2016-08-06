import citibike_trips

db = citibike_trips.DataStore(uri="mongodb://localhost:27017")
print(db.get_station_bikeset("72", 'outbound bike trip indices'))
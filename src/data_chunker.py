"""
Runnable script which generates a chunk of the data necessary to build up the June 22 data database, that which
serves the front-end visualization.

It's a chunker because we can only run 2500 Google Directions API queries per day. More if you have access to
multiple people's keys.
"""

import citibike_trips
import googlemaps
import pandas as pd
import random
from tqdm import tqdm


def main():
    key = input("Enter a valid Google Direction API Key: ")
    client = googlemaps.Client(key=key)
    uri = input("Enter a valid MongoDB connection URI: ")
    db = citibike_trips.DataStore(uri=uri)
    n = input("How many trips do you want to geocode (daily API limit is 2500): ")
    # While testing.
    # db.delete_all()
    # End testing.
    try:
        all_data = pd.read_csv("../data/final/all_june_22_citibike_trips.csv", index_col=0)
        keys_already_stored = db.get_all_trip_ids()
        fresh_trip_indices = set(all_data.index).difference(keys_already_stored)
        if len(fresh_trip_indices) == 0:
            print("No more data left to process!")
        else:
            print("There are {0} trips already in the database.".format(len(keys_already_stored)))
            print("There are {0} trips left to process.".format(len(fresh_trip_indices)))
            print("Running job...")
            ids_to_insert = random.sample(fresh_trip_indices, min(int(n), len(fresh_trip_indices)))
            trips_to_process = all_data.ix[ids_to_insert]
            for trip_id, trip in tqdm(trips_to_process.iterrows()):
                trip.name = trip_id
                try:
                    if trip['usertype'] == 'Rebalancing':
                        citibike_trips.RebalancingTrip(trip, client).to_mongodb(db)
                    else:
                        citibike_trips.BikeTrip(trip, client).to_mongodb(db)
                # Sometimes a trip with impossible coordinates is passed---e.g. it appears that a few CitiBikes
                # take a ferry ride between Governer's Island and mainland Manhattan. For these cases we bake in an
                # exception clause.
                except:
                    pass
    finally:
        db.close()
        print("Done.")


if __name__ == '__main__':
    main()

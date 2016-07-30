"""
Runnable script which generates a chunk of the data necessary to build up the June 22 data database, that which
serves the front-end visualization.

It's a chunker because we can only run 2500 Google Directions API queries per day. More if you have access to
multiple people's keys.
"""

import citibike_trips
import googlemaps
import pandas as pd


def main():
    key = input("Enter a valid Google Direction API Key: ")
    client = googlemaps.Client(key=key)
    uri = input("Enter a valid MongoDB connection URI: ")
    db = citibike_trips.DataStore(uri=uri)
    n = input("How many trips do you want to geocode (daily API limit is 2500): ")
    all_data = pd.read_csv("../data/final/all_june_22_citibike_trips.csv")
    keys_already_stored = None  # TODO: Continue implementation.
    # STOP: This would take too long.


if __name__ == '__main__':
    main()

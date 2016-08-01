"""
Runnable script which generates a JSON dump of a given number of random trips from the datastore.
"""

import citibike_trips
import pandas as pd


def main():
    uri = input("Enter a valid MongoDB connection URI: ")
    db = citibike_trips.DataStore(uri=uri)
    n = int(input("How many trips do you want to sample: "))
    f = input("Where do you want to store this: ")
    try:
        # all_data = pd.read_csv("../data/final/all_june_22_citibike_trips.csv", index_col=0)
        trips = db.get_all_trip_ids()
        if len(trips) < n:
            raise IOError("There are but {0} trips stored, and you asked for {1} ya knucklehead".format(trips, n))
        else:
            trips = db.sample(n)
            trips = [trip['geometry']['coordinates'] for trip in trips]
            with open(f, "w") as f:
                f.write(str(trips))
    finally:
        db.close()


if __name__ == '__main__':
    main()

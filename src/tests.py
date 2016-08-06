"""
READS A REWRITE

Unittest module.
"""

import unittest

import pandas as pd
import pymongo
import citibike_trips
from datetime import datetime


class DataLocalizationTest(unittest.TestCase):

    def testMonthlyDataLocalization(self):
        raw_data = citibike_trips.get_raw_trip_data(year=2015, month=3)
        self.assertTrue(isinstance(raw_data, pd.DataFrame) and len(raw_data) > 0)

    def testBikeWeekSample(self):
        n = 25
        result = citibike_trips.select_random_bike_week_from_2015_containing_n_plus_trips(n)
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertTrue(len(result) >= n)


class BikeTest(unittest.TestCase):

    def setUp(self):
        self.client = citibike_trips.initialize_google_client(filename="../credentials/google_maps_api_key.json")
        # A randomly selected trip for testing, generated with `sample_trips.head(1).values[0].to_dict()`.
        self.test_srs = pd.Series({
            'Index': 124633,
            'bikeid': 17609.0,
            'birth year': 1985.0,
            'end station id': 243.0,
            'end station latitude': 40.688226,
            'end station longitude': -73.979382000000001,
            'end station name': 'Fulton St & Rockwell Pl',
            'gender': 1.0,
            'start station id': 262.0,
            'start station latitude': 40.6917823,
            'start station longitude': -73.973729900000023,
            'start station name': 'Washington Park',
            'starttime': datetime(2016, 3, 10, 8, 2, 58),
            'stoptime': datetime(2016, 3, 10, 8, 6, 36),
            'tripduration': 218.0,
            'usertype': 'Subscriber'
        })

    def testInitialization(self):
        citibike_trips.BikeTrip(self.test_srs, self.client)

    def testCoords(self):
        coord_list = citibike_trips.BikeTrip.get_bike_trip_path([40.76727216, -73.99392888], [40.701907, -74.013942],
                                                                self.client)
        self.assertTrue(len(coord_list) > 0)


class RebalancingTest(unittest.TestCase):

    def setUp(self):
        self.client = citibike_trips.initialize_google_client(filename="../credentials/google_maps_api_key.json")
        # Randomly selected rebalanced delta, generated via `sample_trips.head(2)` (note: bikeids do not match)
        self.test_rebalanced_delta = pd.read_csv("../data/part_1/rebalanced_sample.csv")
        # Randomly selected non-rebalanced delta, generated via:
        # >>> sample_trips[sample_trips['bikeid'] == 17609].sort_values(by="starttime").head(2)
        # The bikeid is just a random one.
        self.test_non_rebalanced_delta = pd.read_csv("../data/part_1/non_rebalanced_sample.csv")

    def testCoords(self):
        coord_list, time_estimate = citibike_trips.RebalancingTrip.get_rebalancing_trip_path_time_estimate_tuple(
            [40.76727216, -73.99392888], [40.701907,-74.013942], self.client
        )
        self.assertTrue(len(coord_list) > 0)

    def testRebalancedDetection(self):
        self.assertTrue(citibike_trips.RebalancingTrip.rebalanced(self.test_rebalanced_delta) == True)
        self.assertTrue(citibike_trips.RebalancingTrip.rebalanced(self.test_non_rebalanced_delta) == False)

    def testInitialization(self):
        rebalancing_trip = citibike_trips.RebalancingTrip(self.test_rebalanced_delta, self.client)
        self.assertTrue(len(rebalancing_trip.data['geometry']['coordinates']) > 0)


class DataStoreTest(unittest.TestCase):

    def setUp(self):
        try:
            self.db = citibike_trips.DataStore(uri="mongodb://localhost:27017")  # default URI
        except pymongo.errors.ServerSelectionTimeoutError as err:
            raise err

    def testSomething(self):
        pass

    def tearDown(self):
        self.db.close()


if __name__ == "__main__":
    unittest.main()
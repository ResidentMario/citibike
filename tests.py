import unittest
import citibike_trips
import pandas as pd


# class DataLocalizationTest(unittest.TestCase):
#
#     def testMonthlyDataLocalization(self):
#         """
#         Tests get_raw_trip_data(); primarily a networking test thereof.
#         """
#         raw_data = citibike_trips.get_raw_trip_data(year=2015, month=3)
#         self.assertTrue(isinstance(raw_data, pd.DataFrame) and len(raw_data) > 0)
#
#     def testBikeWeekSample(self):
#         """
#         Tests select_random_bike_week_from_2015_containing_n_plus_trips().
#         """
#         n = 25
#         result = citibike_trips.select_random_bike_week_from_2015_containing_n_plus_trips(n)
#         self.assertTrue(isinstance(result, pd.DataFrame))
#         self.assertTrue(len(result) >= n)


class BikeTest(unittest.TestCase):

    def setUp(self):
        self.client = citibike_trips.gmaps
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
            'starttime': '2016-03-10 08:02:58',
            'stoptime': '2016-03-10 08:06:36',
            'tripduration': 218.0,
            'usertype': 'Subscriber'
        })

    def testInitialization(self):
        # TODO: Mock the API call (see above).
        citibike_trips.BikeTrip(self.test_srs)

    # def testCoords(self):
    #     # TODO: Mock the API call.
    #     coord_list = citibike_trips.BikeTrip.get_bike_trip_path([40.76727216,-73.99392888], [40.701907,-74.013942],
    #                                                              self.client)
    #     self.assertTrue(len(coord_list) > 0)

if __name__ == "__main__":
    unittest.main()
import requests
import zipfile
import os
import io
import pandas as pd
import googlemaps
import json
import numpy as np
import geojson
from polyline.codec import PolylineCodec
from datetime import datetime, timedelta
from pymongo import MongoClient
import random


#####################
# Google API Client #
#####################

# This is a top-level resource because many different classes need it.


def initialize_google_client(filename='google_maps_api_key.json'):
    """
    Imports the Google Directions API credentials used for polyline generation, returning a usable client.

    You need to have your Google Maps API credentials stored locally as `google_maps_api_key.json` (or whatever
    alternative filename you choose) using the following format for the next few lines to work:

    { "key": "..." }

    See the Google Developer Console (https://console.developers.google.com/) for information getting your own API
    key. Note that you will need a *browser key*, specifically, not the similar but functionally different *server
    key*.

    Parameters
    ----------
    filename: str
        The file the credentials are stored in.

    Returns
    -------
    A googlemaps.Client with credentials initialized and ready for action.
    """
    if filename in [f for f in os.listdir('.') if os.path.isfile(f)]:
        data = json.load(open(filename))['key']
        return googlemaps.Client(key=data)
    else:
        raise IOError('This API requires a Google Maps credentials token to work. Did you forget to define one?')


gmaps = initialize_google_client()


#########################
# Raw Data Localization #
#########################


def get_raw_trip_data(month=None, year=None):
    """
    Downloads, unzips, and saves locally the CitiBike trips data for the given month.

    The URIs used by CitiBike are of the form "https://s3.amazonaws.com/tripdata/201603-citibike-tripdata.zip". I
    preserve this format locally---so 201603-citibike-tripdata.csv, 201412-citibike-tripdata.csv, and so on.

    CitiBike goes back only to July 2013 (as of writing---though this is unlikely to change), so it is expected that
    user input refer to a month there or after. Additionally note that it takes up to a month for the most recent
    month's data to be uploaded, so reliably getting for example "last week's" data is out of the question.

    This method checks whether or not the file is already available locally. It avoids re-downloading if it is.

    Data is stored is a subdirectory: "data/201503.csv", for example.

    Parameters
    ----------
    month: int
        The month whose data is being localized, in integer format.

    year: int
        The year whose data is being localized, in integer format.
    """
    filename = '{0}{1}-citibike-tripdata'.format(year, str(month).zfill(2))
    r = requests.get('https://s3.amazonaws.com/tripdata/{0}.zip'.format(filename))
    with zipfile.ZipFile(io.BytesIO(r.content)) as ar:
        trip_data = pd.read_csv(ar.open('{0}.csv'.format(filename)))
        return trip_data


def select_random_bike_week_from_2015_containing_n_plus_trips(n=25):
    """
    Selects and returns a random bike-week, starting on a Sunday, corresponding with a bike-week in at least the
    50th percentile of bike-weeks overall.

    The data that this method returns is a raw slice of the basic trip data, containing CSV-formatted trip starting
    points and ending points, missing in-between re-balancing trips. To be use-able in our visualization there is
    still so much work that must be done to get the selection into the desired GeoJSON shape.

    Parameters
    ----------
    n: int
        The number of trips that will be the minimum for the bike-weeks returned.

    Returns
    -------
    A `pandas` DataFrame containing the raw selected bike-week data.
    """
    # Select a random week.
    date_ranges = np.arange(np.datetime64('2015-01-04'),
                            np.datetime64('2016-01-01'),
                            step=np.timedelta64(1, 'W'))
    start = np.random.choice(date_ranges)
    end = start + np.timedelta64(1, 'W')
    # Get the data for that week.
    # The following is too slow:
    # trip_data['starttime'] = trip_data['starttime'].map(
    #     lambda x: np.datetime64(datetime.strptime(x, "%m/%d/%Y %H:%M:%S")))
    # trip_data['stoptime'] = trip_data['stoptime'].map(
    #     lambda x: np.datetime64(datetime.strptime(x, "%m/%d/%Y %H:%M:%S")))
    start_month = start.astype(datetime).month
    end_month = end.astype(datetime).month
    if start_month == end_month:
        trip_data = get_raw_trip_data(year=2015, month=start_month)
    else:
        trip_data = pd.concat([get_raw_trip_data(year=2015, month=start_month),
                               get_raw_trip_data(year=2015, month=end_month)])
    # Both a lambda function using strptime and the pandas to_datetime method are unacceptably slow for converting
    # the dates to a usable comparative format.
    print("Converting strings to datetimes...")
    trip_data['starttime'] = pd.to_datetime(trip_data['starttime'], infer_datetime_format=True)
    trip_data['stoptime'] = pd.to_datetime(trip_data['stoptime'], infer_datetime_format=True)
    # Extract that week from the monthly data.
    selected_week = trip_data[(trip_data['starttime'] > start) & (trip_data['stoptime'] < end)]
    # Pick a bike with more than 25 trips and return it.
    value_counts = selected_week['bikeid'].value_counts()
    selectable_bike_ids = value_counts[value_counts > n].index
    chosen_bike_id = np.random.choice(selectable_bike_ids)
    return selected_week[(selected_week['bikeid'] == chosen_bike_id)]


class BikeTrip:
    """
    Class encoding a single bike trip. Wrapper of a GeoJSON FeatureCollection.
    """
    def __init__(self, raw_trip):
        """
        :param raw_trip: A pandas Series, taken the from the raw CitiBike data, of a bike trip.
        """
        path = self.get_bike_trip_path([raw_trip['start station latitude'],
                                        raw_trip['start station longitude']],
                                       [raw_trip['end station latitude'],
                                        raw_trip['end station longitude']], gmaps)
        props = raw_trip.to_dict()
        # Because mongodb does not understand numpy data types, in order for this class to be compatible with our
        # data store we have to cast all of the object stored as numpy types back into base Python types. This has to
        #  be done manually.
        for p in ['bikeid', 'birth year', 'gender',
                  'end station id', 'end station longitude', 'end station latitude',
                  'start station id', 'start station longitude', 'start station latitude',
                  'tripduration']:
            props[p] = int(props[p]) if pd.notnull(props[p]) else 0
        for p in ['starttime', 'stoptime']:
            props[p] = props[p].strftime("%Y-%d-%m %H:%M:%S")
        self.data = geojson.Feature(geometry=geojson.LineString(path, properties=props))

    @staticmethod
    def get_bike_trip_path(start, end, client):
        """
        Given a bike trip starting point, a bike trip ending point, and a Google Maps client, returns a list of
        coordinates corresponding with the path that that bike probably took, as reported by Google Maps.

        This low-level but highly compact method is wrapped by far more complex operational methods upstream.

        Parameters
        ----------
        start: list
            The starting point coordinates, in [latitude, longitude] (or [y, x]) format.
        end: list
            The end point coordinates, in [latitude, longitude] (or [y, x]) format.
        client: googlemaps.Client
            A `googlemaps.Client` instance, as returned by e.g. `import_google_credentials()`.

        Returns
        -------
        The list of [latitude, longitude] coordinates for the given bike trip.
        """
        codec = PolylineCodec()
        req = client.directions(start, end, mode='bicycling')
        polylines = [step['polyline']['points'] for step in [leg['steps'] for leg in req[0]['legs']][0]]
        coords = []
        for polyline in polylines:
            coords += codec.decode(polyline)
        return coords


class RebalancingTrip:
    """
    Class encoding a single re-balancing trip. Wrapper of a GeoJSON FeatureCollection.
    """

    def __init__(self, delta_df, client):
        """
        Given a "delta DataFrame"---that is, one corresponding with a pair of trips with different starting and ending
        points, as checked for and/or returned by `rebalanced()` and `get_list_of_rebalancing_frames_from_bike_week()`
        ---returns a fully packaged GeoJSON representation

        There are a lot of intermediate steps to this process. The GeoJSON representation must be built from scratch.
        Start time and stop time are computed to be exactly in the middle of the two surrounding trips.

        This method implements `get_rebalancing_trip_path_time_estimate_tuple()` and `rebalanced()`.

        Compare with the far simpler `bike_tripper()`, which does the same thing for the it-turns-out far simpler case of
        actual bike trips.

        Parameters
        ----------
        delta_df: pd.DataFrame
            A pandas DataFrame containing a delta DataFrame (two adjacent bike trips with different start and end points).
        client: googlemaps.Client
            A `googlemaps.Client` instance, as returned by e.g. `import_google_credentials()`.


        Returns
        -------
        The list of pd.DataFrame objects corresponding with the aforementioned rebalancing trip deltas.
        """
        # delta_df['starttime'] = delta_df['starttime'].astype(datetime)
        # delta_df['stoptime'] = delta_df['stoptime'].astype(datetime)
        # f = lambda x: datetime.strptime(x, "%Y-%d-%m %H:%M:%S")
        # delta_df['starttime'] = delta_df['starttime'].map(f)
        # delta_df['stoptime'] = delta_df['stoptime'].map(f)
        start_point = delta_df.iloc[0]
        end_point = delta_df.iloc[1]
        start_lat, start_long = start_point[["end station latitude", "end station longitude"]]
        end_lat, end_long = end_point[["start station latitude", "start station longitude"]]
        coords, time_estimate_mins = self.get_rebalancing_trip_path_time_estimate_tuple([40.76727216, -73.99392888],
                                                                                   [40.701907, -74.013942], client)
        midpoint_time = end_point['starttime'] + ((end_point['starttime'] - start_point['stoptime']) / 2)
        rebalancing_start_time = midpoint_time - timedelta(minutes=time_estimate_mins / 2)
        rebalancing_end_time = midpoint_time + timedelta(minutes=time_estimate_mins / 2)
        if rebalancing_start_time < start_point['stoptime']:
            rebalancing_start_time = start_point['stoptime']
        if rebalancing_end_time > end_point['starttime']:
            rebalancing_end_time = end_point['starttime']
        # Explicit casts are due to mongodb limitations, see BikeTrip above.
        attributes = {
            "tripduration": int(time_estimate_mins * 60),
            "start station id": int(start_point['end station id']),
            "end station id": int(end_point['start station id']),
            "start station name": start_point['end station name'],
            "end station name": end_point['start station name'],
            "bikeid": int(start_point["bikeid"]),
            "usertype": "Rebalancing",
            "birth year": 0,
            "gender": 3,
            "start station latitude": int(start_lat),
            "start station longitude": int(start_long),
            "end station latitude": int(end_lat),
            "end station longitude": int(end_long),
            "starttime": rebalancing_start_time.strftime("%Y-%d-%m %H:%M:%S"),
            "stoptime": rebalancing_end_time.strftime("%Y-%d-%m %H:%M:%S")
        }
        self.data = geojson.Feature(geometry=geojson.LineString(coords, properties=attributes))

    @staticmethod
    def get_rebalancing_trip_path_time_estimate_tuple(start, end, client):
        """
        Given a re-balancing trip starting point, a re-balancing trip ending point, and a Google Maps client,
        returns a list of coordinates corresponding with the path that van probably took, as reported by Google Maps,
        as well as a time estimate.

        The need to return a tuple containing not just the path (as in the case of very similar `bike_tripper`) stems
        from the fact that whereas for bikes we have a precise time in transit, we have no such information for
        rebalancing van trips, meaning that we have to calculate the time taken and timing of such trips ourselves.

        Parameters
        ----------
        start: list
            The starting point coordinates, in [latitude, longitude] (or [y, x]) format.
        end: list
            The end point coordinates, in [latitude, longitude] (or [y, x]) format.
        client: googlemaps.Client
            A `googlemaps.Client` instance, as returned by e.g. `import_google_credentials()`.

        Returns
        -------
        The list of [latitude, longitude] coordinates for the given bike trip.
        """
        codec = PolylineCodec()
        req = client.directions(start, end, mode='driving')
        # Get the time estimates.
        # Raw time estimate results are strings of the form "1 min", "5 mins", "1 hour 5 mins", "2 hours 5 mins", etc.
        time_estimates_raw = [step['duration']['text'] for step in [leg['steps'] for leg in req[0]['legs']][0]]
        time_estimate_mins = 0
        for time_estimate_raw in time_estimates_raw:
            # Can we really get an hour+ estimate biking within the city? Possibly not but I won't risk it.
            if "min" in time_estimate_raw and "hour" not in time_estimate_raw:
                time_estimate_mins += int(time_estimate_raw.split(" ")[0])
            elif "hour" in time_estimate_raw:
                time_estimate_mins += 60 * int(time_estimate_raw.split(" ")[0])
                if "min" in time_estimate_raw:
                    time_estimate_mins += int(time_estimate_raw.split(" ")[2])
                else:
                    # Uh-oh.
                    pass
        # Get the polylines.
        polylines = [step['polyline']['points'] for step in [leg['steps'] for leg in req[0]['legs']][0]]
        coords = []
        for polyline in polylines:
            coords += codec.decode(polyline)
        # Return
        return coords, time_estimate_mins

    @staticmethod
    def rebalanced(delta_df):
        """
        Parameters
        ----------
        delta_df: pd.DataFrame
            Two bike trips.

        Returns
        -------
        Returns True if the bike was rebalanced in between the trips, False otherwise.
        """
        ind_1, ind_2 = delta_df.index.values
        if delta_df.ix[ind_1, 'end station id'] == delta_df.ix[ind_2, 'start station id']:
            return False
        else:
            return True


class DataStore:
    """
    Class encoding the Citibike data storage layer. This file is a direct copy of the one in the `citibike` project
    repository.
    """

    def __init__(self, credentials_file="mlab_instance_api_key.json"):
        """
        Initializes a connection to an mLab MongoDB client.
        """
        with open(credentials_file) as cred:
            uri = json.load(cred)['uri']
        self.client = MongoClient(uri)
        self.bikeweeks = self.client['citibike']['bike-weeks']

    def insert(self, bikeweek):
        """
        Insert a single bikeweek into the database. Returns an inspectable result object, with e.g. `acknowledged` and
        `inserted_id` fields.
        """
        return self.bikeweeks.insert_one(bikeweek)

    def delete_all(self):
        """
        Clears the entire database. Only useful for testing. Don't do this actually.
        """
        self.bikeweeks.delete_many({})

    def delete(self, document_id):
        """
        Deletes a single entry, by document id (as returned by e.g. `result.inserted_id`). only useful for testing.
        """
        return self.bikeweeks.delete_one({'_id': document_id})

    def select_all(self):
        """
        Returns all documents in the data store.
        """
        return self.bikeweeks.find({})

    def sample(self):
        """
        Returns a single random bikeweek from storage.
        """
        r = random.randint(0, self.bikeweeks.count({}) - 1)
        sample = self.bikeweeks.find({}).limit(1).skip(r).next()
        # _id is a BSON parameter which can technically be extended, but since I don't need the object anyway I can
        # safely delete it.
        del sample['_id']
        return sample

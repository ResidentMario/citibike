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
from pymongo.errors import ServerSelectionTimeoutError
import pymongo
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
    if os.path.isfile(filename):
        with open(filename) as f:
            data = json.load(f)['key']
        return googlemaps.Client(key=data)
    else:
        raise IOError(
            'This API requires a Google Maps credentials token to work. Did you forget to define one?')


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
    start_month = start.astype(datetime).month
    end_month = end.astype(datetime).month
    if start_month == end_month:
        trip_data = get_raw_trip_data(year=2015, month=start_month)
    else:
        trip_data = pd.concat([get_raw_trip_data(year=2015, month=start_month),
                               get_raw_trip_data(year=2015, month=end_month)])
    # This conversion is very slow.
    # print("Converting strings to datetimes...")
    if isinstance(trip_data['starttime'][0], str):
        trip_data['starttime'] = pd.to_datetime(trip_data['starttime'], infer_datetime_format=True)
    if isinstance(trip_data['stoptime'][0], str):
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
    Class encoding a single bike trip. Wrapper of a GeoJSON FeatureCollection with lazily loaded geometry.
    """
    def __init__(self, raw_trip, client):
        """
        Initializes a BikeTrip. Expects a raw trip from the dataset as input---this should be in the form of a
        pd.Series with a `name` set to be equal to the trip's id in the processed dataset.
        """
        props = raw_trip.to_dict()
        # Because mongodb does not understand numpy data types, in order for this class to be compatible with our
        # data store we have to cast all of the object stored as numpy types back into base Python types. This has to
        #  be done manually.
        for p in ['bikeid', 'birth year', 'gender',
                  'end station id', 'end station longitude', 'end station latitude',
                  'start station id', 'start station longitude', 'start station latitude',
                  'tripduration']:
            props[p] = float(props[p]) if pd.notnull(props[p]) else 0
        # Store the id both in the document store...
        props['tripid'] = int(raw_trip.name)
        # And in the Python object, because we'll need easy access to it in order to pass it to the MongoDB id list.
        self.id = props['tripid']
        self.data = geojson.Feature(geometry=geojson.LineString(), properties=props)
        self.client = client

    def __getitem__(self, item):
        """
        Makes accessing properties more convenient.

        Implements lazy loading of geometry data.
        """
        if item != 'coordinates':
            return self.data['properties'][item]
        else:
            current_geom = self.data['geometry']['coordinates']
            if len(current_geom) != 0:
                return current_geom
            else:
                path = self.get_bike_trip_path([self['start station latitude'],
                                                self['start station longitude']],
                                               [self['end station latitude'],
                                                self['end station longitude']], self.client)
                self.data['geometry']['coordinates'] = path
                return path

    @staticmethod
    def get_bike_trip_path(start, end, client):
        """
        Given a bike trip starting point, a bike trip ending point, and a Google Maps client, returns a list of
        coordinates corresponding with the path that that bike probably took, as reported by Google Maps.

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

    def to_mongodb(self, datastore):
        datastore.insert_trip(self)


class RebalancingTrip:
    """
    Class encoding a single bike trip. Wrapper of a GeoJSON FeatureCollection. Unlike BikeId, not lazily loaded.
    """

    def __init__(self, delta, client):
        """
        This class initializer takes one of two different kinds of inputs in df, plus a valid Google maps client as
        the client paramater.

        The first kind of input---the one written for the purposes of generating bike-weeks in the original revision
        of this codebase---is what I call a delta DataFrame. This is DataFrame containing two Series corresponding
        with two adjacent trips in which the end point of the first trip does not match the start point of the
        second, implying that a rebalancing trip was made in between the two points.

        The second kind of input---generated later---is a single Series corresponding with a single precalculated
        rebalancing trip. That is, the expected input in this case is a Series with all of the expected parameters
        of the core dataset pre-filled. Note that in this case the name of the Series---the Series.name
        parameter---MUST be a unique id for the trip in question.

        Generating the data I need using the second mode requires significant preprocessing efforts but simplifies
        the codebase overall because it allows to know, when writing the data to the data store, how many more trips
        I need to run through the geocoder.

        Pre-filling requires running get_rebalancing_trip_path_time_estimate_tuple() head of time; for more details see
        notebook 06.

        Either way this class initializer assigns to the object a fully packaged GeoJSON representation. There are a
        lot of intermediate steps to this process. The GeoJSON representation must be built from scratch. Start time
        and stop time are computed to be exactly in the middle of the two surrounding trips.

        Compare with the far simpler `bike_tripper()`, which does the same thing for the it-turns-out far simpler case of
        actual bike trips.

        Parameters
        ----------
        delta: pd.DataFrame or pd.Series
            A pandas DataFrame containing a delta DataFrame (two adjacent bike trips with different start and end
            points). Alternatively, a single pandas Series containing the preprocessed trip.
        client: googlemaps.Client
            A `googlemaps.Client` instance, as returned by e.g. `import_google_credentials()`.
        """
        if isinstance(delta, pd.DataFrame):
            # First initialization type.
            start_point = delta.iloc[0]
            end_point = delta.iloc[1]
            for point in [start_point, end_point]:
                for time in ['starttime', 'stoptime']:
                    if isinstance(point[time], str):
                        point[time] = pd.to_datetime(point[time], infer_datetime_format=True)
            start_lat, start_long = start_point[["end station latitude", "end station longitude"]]
            end_lat, end_long = end_point[["start station latitude", "start station longitude"]]
            coords, time_estimate_mins = self.get_rebalancing_trip_path_time_estimate_tuple([start_lat, start_long],
                                                                                            [end_lat, end_long], client)
            midpoint_time = start_point['stoptime'] + ((end_point['starttime'] - start_point['stoptime']) / 2)
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
                "start station latitude": float(start_lat),
                "start station longitude": float(start_long),
                "end station latitude": float(end_lat),
                "end station longitude": float(end_long),
                "starttime": rebalancing_start_time.strftime("%m/%d/%Y %H:%M:%S").lstrip('0'),
                "stoptime": rebalancing_end_time.strftime("%m/%d/%Y %H:%M:%S").lstrip('0'),
                "tripid": delta.index[0]
            }
            self.data = geojson.Feature(geometry=geojson.LineString(coords, properties=attributes))
        elif isinstance(delta, pd.Series):
            # Second initialization type.
            coords, _ = self.get_rebalancing_trip_path_time_estimate_tuple([delta["start station latitude"],
                                                                            delta["start station longitude"]],
                                                                           [delta["end station latitude"],
                                                                            delta["end station longitude"]], client)
            props = delta.to_dict()
            # Store the id both in the document store...
            props['tripid'] = int(delta.name)
            # And in the Python object, because we'll need easy access to it in order to pass it to the MongoDB id list.
            self.id = props['tripid']
            self.data = geojson.Feature(geometry=geojson.LineString(coords), properties=props)

    def __getitem__(self, item):
        """
        Makes accessing properties more convenient.
        """
        if item != 'coordinates':
            return self.data['properties'][item]
        else:
            return self.data['geometry']['coordinates']

    def to_mongodb(self, datastore):
        datastore.insert_trip(self)

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
        return delta_df.ix[ind_1, 'end station id'] != delta_df.ix[ind_2, 'start station id']


class DataStore:
    """
    Class encoding the Citibike data storage layer.
    """

    # INITIALIZATION
    def __init__(self, uri):
        """
        Initializes a connection to a MongoDB database.
        """
        try:
            client = MongoClient(uri)
            client.server_info()
        except ServerSelectionTimeoutError as err:
            raise err
        self.client = client
        # If an index on (start station id, end station id) pairs have not already been created, create it.
        # This operation is idempotent, if the index already exists it does nothing.
        self.client['citibike']['trip-geometries'].create_index([('start station id', pymongo.ASCENDING),
                                                                 ('end station id', pymongo.ASCENDING)])

    # INSERTION
    def update_trip_id_list(self, new_ids):
        """
        Updates the list of trip ids stored in the "citibike-keys" store to include the additional ones.
        """
        unique_ids = set(self.get_all_trip_ids()).union(new_ids)
        self.client['citibike']['citibike-trip-ids'].update({'name': 'id-list'},
                                                            {'name': 'id-list', 'id-list': list(unique_ids)},
                                                            upsert=True)

    def insert_trip(self, trip):
        """
        Inserts a single trip (either a BikeTrip or a RebalancingTrip) into the database.
        """
        path = self.client['citibike']['trip-geometries'].find_one({
            'start station id': trip['start station id'],
            'end station id': trip['end station id']
        })
        reverse_path = self.client['citibike']['trip-geometries'].find_one({
            'start station id': trip['end station id'],
            'end station id': trip['start station id']
        })
        if path:
            # The geometry has already been stored in the database. Do nothing.
            pass
        elif reverse_path:
            # The geometry has already been stored in the database, just backwards. Do nothing.
            pass
        else:
            # The geometry has not already been stored in the database, so insert it (if it's a BikeTrip!).
            if isinstance(trip, BikeTrip):
                self.client['citibike']['trip-geometries'].insert_one({
                    'start station id': trip['start station id'],
                    'end station id': trip['end station id'],
                    'coordinates': trip['coordinates']
                })
            else:  # I don't cache rebalancing trip geometry; that's stored inline.
                pass
        self.client['citibike']['citibike-trips'].insert_one(trip.data)
        self.update_trip_id_list([trip.id])

    # GETTERS
    def get_trips_by_ids(self, tripset):
        """
        Returns a list of trips selected by ID.

        If the trip is missing this method returns None for that trip.
        """
        # First find all trips which are in our id list.
        trips = list(self.client['citibike']['citibike-trips'].find({'properties.tripid': {"$in": tripset}}))
        # Create a geoms list, which will store a list of requested geometries. The reason for this variable is that
        # requesting these geometries one at a time, as would be necessary otherwise, is inefficient; it is better if
        # we can request them all at once. Some additional folds to keep in mind here:
        # (1) Rebalancing trips, which occur on vans, not on bicycles, store their geometry inline with their
        #     definition. (in retrospect this was probably a mistake to do, but whatever)
        # (2) If the database does not have the geometry for the start-station-->end-station orientation of the trip,
        #     there is an approximately-equal chance that it instead has the end-station-->start-station orientation
        #     stored. Thus we need to associate data twice, once for each direction.
        # (3) This code should work with partial data, e.g. while a data storage layer is being built.
        # Split into rebalancing trips which don't need addressing and regular ones which do.
        rebalancing_trips = [trip for trip in trips if trip['properties']['usertype'] == 'Rebalancing']
        regular_trips = [trip for trip in trips if trip['properties']['usertype'] != 'Rebalancing']
        # Create a list of valid geometries that we want.
        requested_geometries = [[trip['properties']['start station id'],
                                 trip['properties']['end station id']] for trip in regular_trips]
        requested_geometries_backwards = [geom[::-1] for geom in requested_geometries]
        # At this point we have a list of station geometries that we want of the form [[station_A, station_B], [..]].
        # Next we build the conditional logical string that we throw at MongoDB to generate our geometry list. This
        # needs to be a pairwise request which asks that both of the properties that we want are equal to what we
        # want them to be. Here's an example pymongo snippet for something like this that you can plug into IPython
        # and run:
        # >>> import json
        # >>> from pymongo import MongoClient
        # >>> mongo_uri = json.load(open("../credentials/mlab_instance_api_key.json"))['uri']
        # >>> client = MongoClient(mongo_uri)
        # >>> len(list(client['citibike']['trip-geometries'].find({'$or':
        # >>>       [{'start station id': 3078, 'end station id': 3100},
        # >>>        {'start station id': 410, 'end station id': 3148}]})))
        pymongo_request_string = {'$or': [{'start station id': sid, 'end station id': eid} for sid, eid in
                                          requested_geometries + requested_geometries_backwards]}
        database_geometries = list(self.client['citibike']['trip-geometries'].find(pymongo_request_string))
        database_geometry_start_ends = [(geom['start station id'], geom['end station id']) for geom in
                                        database_geometries]
        # Now we plug the geometries we got back into our triplist. Note that we must take into account the important
        # subtlety that if multiple trips in the requested $or set have the same geometry, it will only be returned
        # once, which means that we can't expect the indices returned by our request to match the indices of our
        # geometry series! Instead we match them by key.
        for trip in regular_trips:
            start_end = (trip['properties']['start station id'], trip['properties']['end station id'])
            try:
                trip['geometry']['coordinates'] = database_geometries[database_geometry_start_ends.index(start_end)][
                    'coordinates']
            except ValueError:
                try:
                    trip['geometry']['coordinates'] = database_geometries[database_geometry_start_ends.index(
                        start_end[::-1])]['coordinates'][::-1]
                except ValueError:
                    pass
                    # raise  # This is OK if the database is full, but not OK if it isn't.
        trips = rebalancing_trips + regular_trips
        for trip in trips:
            del trip['_id']
        return trips
        # Speedup relative to using `get_trip_by_id`: get_trip_by_id() returns ~25 trips/second, with a ~2 minute (!)
        # wait time for the 3376 trips returned by Penn Station Valet (timing according to the Firefox web console,
        # so it includes packaging and downloading the request). Using this method instead I found:
        # >>> %timeit list(db.get_trips_by_ids(np.random.choice(data.index.values, size=1000).tolist()))
        #     1 loop, best of 3: 1.95 s per loop
        # >>> %timeit list(db.get_trips_by_ids(np.random.choice(data.index.values, size=10000).tolist()))
        #     1 loop, best of 3: 24.4 s per loop
        #
        # This translates to ~8 seconds for the example of Penn Station Valet.

    def get_trip_by_id(self, tripid):
        """
        Returns a trip selected by its ID.

        If the trip is missing this method returns None.
        """
        trip = self.client['citibike']['citibike-trips'].find_one({"properties.tripid": tripid})
        if trip:
            del trip['_id']
            if trip['properties']['usertype'] == "Rebalancing":
                return trip
            else:
                path = self.client['citibike']['trip-geometries'].find_one({
                    'start station id': trip['properties']['start station id'],
                    'end station id': trip['properties']['end station id']
                })
                if path:
                    coordinates = path['coordinates']
                else:
                    path = self.client['citibike']['trip-geometries'].find_one({
                        'start station id': trip['properties']['end station id'],
                        'end station id': trip['properties']['start station id']
                    })
                    coordinates = path['coordinates'][::-1]
                trip['geometry']['coordinates'] = coordinates
                return trip
        else:
            return None

    def get_station_bikeset(self, station_id, mode):
        """
        This is it, folks---this is the core method which gets called when the front-end requests a station bikeset
        off of an id. Everything else that's been implemented here is in support of this ultimate end goal.
        """
        tripset = self.client['citibike']['station-indices'].find_one({'station id': str(station_id)})['tripsets'][mode]
        return self.get_trips_by_ids(tripset)

    # UTILITY
    def delete_all(self):
        """
        Flushes the entire database down the toilet. Only useful for testing. Don't do this actually.
        """
        self.client['citibike']['citibike-trips'].delete_many({})
        self.client['citibike']['citibike-trip-ids'].delete_many({})
        self.client['citibike']['citibike-geometries'].delete_many({})

    def get_all_trip_ids(self):
        """
        Returns all of the trip ids stored in the "citibike-keys" store.

        Note: this does not associate any geometries with those trips!
        """
        try:
            keystore = self.client['citibike']['citibike-trip-ids'].find({'name': 'id-list'}).next()
            return keystore['id-list']
        except StopIteration:  # empty database
            return []

    def sample(self, n):
        """
        Samples n random trips from the data store.
        """
        samples = []
        r_s = random.sample(self.get_all_trip_ids(), n)
        for r in r_s:
            samples.append(self.get_trip_by_id(r))
        return samples

    def iter_all(self):
        """
        Returns an iterator cursor which lets you do something to every object in the datastore.
        """
        return self.client['citibike']['citibike-trips'].find({})

    def replace_trip(self, tripid, new_repr):
        """
        Replaces the trip in question with another.
        """
        return self.client['citibike']['citibike-trips'].update({'tripid': tripid}, {"$set": new_repr}, upsert=False)

    def close(self):
        """
        Close the database (pass-through wrapper).
        """
        self.client.close()
"""
Mock module (for now) of the master driver for this project's backend.

Test, test, test!
"""

import requests
import zipfile
import os
import io
import pandas as pd
import googlemaps
import json
import geojson


def check_db_size(number_records_limit=1000, memory_limit=0.5):
    """
    Checks whether or not the bike-week database can still be added to.

    If this methods returns a False flag the expected behavior is to have the bike-week appending script immediately
    stop, as nothing more should be done.

    Parameters
    ----------
    number_records_limit: int
        The numerical cap on the number of records in the database. 1000 is chosen as a somewhat arbitrary value.
    memory_limit: float
        The limit on the size of the database, in gigabytes. Set to 0.5 by defaults because this is the mLab free
        allotment limit.

    Returns
    -------
    If the database can still be appended to---that is, if it is within operational constraints both in terms of its
    memory usage and the number of records it counts---then return True. If not, return False.
    """
    pass


def localize_trip_data(month, year):
    """
    Downloads, unzips, and saves locally the CitiBike trips data for the given month.

    The URIs used by CitiBike are of the form "https://s3.amazonaws.com/tripdata/201603-citibike-tripdata.zip". I
    preserve this format locally---so 201603-citibike-tripdata.csv, 201412-citibike-tripdata.csv, and so on.

    CitiBike goes back only to July 2013 (as of writing---though this is unlikely to change), so it is expected that
    user input refer to a month there or after. Additionally note that it takes up to a month for the most recent
    month's data to be uploaded, so reliably getting for example "last week's" data is out of the question.

    Parameters
    ----------
    month: int
        The month whose data is being localized, in integer format.

    year: int
        The year whose data is being localized, in integer format.
    """
    r = requests.get('https://s3.amazonaws.com/tripdata/{0}{1}-citibike-tripdata.zip'.format(year, str(month).zfill(2)))
    with zipfile.ZipFile(io.BytesIO(r.content)) as ar:
        ar.write('{0}{1}-citibike-tripdata.csv'.format(year, str(month).zfill(2)))


def select_random_bike_week_from_year(trip_data, year=2015, percentile_cutoff=0.5):
    """
    Selects a random bike-week from the given year, with a certain percentile parameter.

    Parameters
    ----------
    trip_data: pd.DataFrame
        The raw trip data bike-weeks will be selected from.
    year: int
        The year that bike-week data will be loaded from.
    percentile_cutoff: float
        In order for a visualization of the data for a CitiBike bike-week to be interesting to watch, the CitiBike
        itself must have moved at least somewhat often. This parameter allows you to specify what (global) percentile a
        bike-week has to fall into in order to be interesting.

        It is set to 0.5 by default.

        At the moment only the 50th percentile (corresponding with a 25-ride bike-week) is implemented. The sampling
        necessary to get a more rigorous has not yet been done.

    Returns
    -------
    A `pandas` DataFrame containing the raw selected bike-week data.
    """
    # TODO: Implement percentiles.
    assert percentile_cutoff == 0.5, "Percentiles other than 50% not yet implemented."
    n = 25
    pass


def import_google_credentials(filename='google_maps_api_key.json'):
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


# TODO: Bunch of other methods defined in the data generation scoping notebook.
# def get_trips(df, bike_id, client):
#     feature_list = []
#     bike_df = df[df['bikeid'] == bike_id].sort_values(by='starttime')
#     for a_minus_1, a in zip(range(len(bike_df) - 1), range(1, len(bike_df))):
#         delta_df = bike_df.iloc[[a_minus_1, a]]
#         ind_1, ind_2 = delta_df.index.values
#         start = delta_df.ix[ind_1]
#         end = delta_df.ix[ind_2]
#         path = get_bike_trip_path([start['start station latitude'], start['start station longitude']],
#                                   [start['end station latitude'], start['end station longitude']],
#                                   client)
#         props = start.to_dict()
#         props['starttime'] = props['starttime'].strftime("%Y-%d-%m %H:%M:%S")
#         props['stoptime'] = props['stoptime'].strftime("%Y-%d-%m %H:%M:%S")
#         feature_list.append(geojson.Feature(geometry=geojson.LineString(path, properties=props)))
#         if rebalanced(delta_df):
#             feature_list.append(get_rebalancing_geojson_repr(delta_df))
#     return geojson.FeatureCollection(feature_list, properties={'bike_id': bike_id})


def geojsonify(raw_bike_week):
    """
    Transforms raw bike-week data into its GeoJSON repr.

    This method wraps a large number of submethods.

    Parameters
    ----------
    raw_bike_week: pd.DataFrame
        Raw bike-week data, as retrieved e.g. via `select_random_bike_week_from_year()`.

    Returns
    -------
    The GeoJSON representation for this bike-week.

    """
    pass


# TODO: Finish mocking up.

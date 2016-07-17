"""
Mock module (for now) of the master driver for this project's backend.

Test, test, test!
"""

import requests


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


def localize_trip_data(month):
    """
    Downloads, unzips, and saves locally the CitiBike trips data for the given month.

    Parameters
    ----------
    month: int
        The month whose data is being localized, in integer format.
    """
    pass


def calculate_ride_number_cutoff():
    # TODO: Maybe hard-code this?
    pass


def select_random_bike_week_from_year(year=2015, percentile_cutoff=0.5):
    """
    Selects a random bike-week from the given year, with a certain percentile parameter.

    Parameters
    ----------
    year: int
        The year that bike-week data will be loaded from.
    percentile_cutoff: float
        In order for a visualization of the data for a CitiBike bike-week to be interesting to watch, the CitiBike
        itself must have moved at least somewhat often. This parameter allows you to specify what (global) percentile a
        bike-week has to fall into in order to be interesting. It is set to 0.5 by default.

    Returns
    -------
    A `pandas` DataFrame containing the raw selected bike-week data.
    """
    pass


def import_google_credentials(filename='google_maps_api_key.json'):
    """
    Imports the Google Directions API credentials used for polyline generation, returning a usable client.

    Parameters
    ----------
    filename: str
        The file the credentials are stored in.

    Returns
    -------
    A googlemaps.Client with credentials initialized and ready for action.
    """

# TODO: Bunch of other methods defined in the data generation scoping notebook.


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

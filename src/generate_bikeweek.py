"""
OBSOLETE

Generates a single sample bike-week and stores it in the database.
"""
import geojson
import pandas as pd

from src.citibike_trips import (initialize_google_client,
                                select_random_bike_week_from_2015_containing_n_plus_trips, BikeTrip, RebalancingTrip,
                                DataStore)


def main():
    """
    The method that actual implements the whole fruckus above.

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
    print("Initializing client...")
    client = initialize_google_client()
    feature_list = []
    print("Generating a random bike week...")
    df = select_random_bike_week_from_2015_containing_n_plus_trips(n=25)
    db = DataStore(credentials_file="../credentials/mlab_instance_api_key.json")
    bike_df = df.sort_values(by='starttime')
    print("Running through the geocoder...")
    for a_minus_1, a in zip(range(len(bike_df) - 1), range(1, len(bike_df))):
        delta_df = bike_df.iloc[[a_minus_1, a]]
        ind_1, ind_2 = delta_df.index.values
        start = delta_df.ix[ind_1]
        # end = delta_df.ix[ind_2]
        bike_trip = BikeTrip(start, client).data
        feature_list.append(bike_trip)
        if RebalancingTrip.rebalanced(delta_df):
            rebalancing_trip = RebalancingTrip(delta_df, client).data
            feature_list.append(rebalancing_trip)
    print("Saving to database...")
    bikeweek = geojson.FeatureCollection(feature_list, properties={'bike_id': int(df.head(1)['bikeid'].values[0])})
    db.insert(bikeweek)
    print("Done!")


if __name__ == "__main__":
    main()
# Life of CitiBike

This repository contains various files that I generated as part of my quest to animate the daily and/or weekly
lifecycle of New York City's CitiBike system. All of the scoping, done in Python using Jupyter notebooks, is here, as
 are various toy datasets generated along the way, and&mdash;critically&mdash;the scripts which generate the
 visualization's backend. The only thing *not* in this repository is the final visualization itself, which is
 optimized for and hosted at my personal website, and can be found [there](https://github.com/ResidentMario/mysite/blob/master/templates/visualizations/life-of-a-citibike.html).

If you are interested in following along, read this technical blog post first. Then, if you're still interested,
start reading the materials in the `notebooks` folder.

Note that station depots and New Jersey locations (on which no data is yet available, annoyingly) have been removed from
this dataset during processing.

## Data dictionary for `june_22_station_metadata.csv`

* `station id` &mdash; The unique identifier for this station.
* `latitude`
* `longitude`
* `station name` &mdash; The station name.
    * Usually the name of an intersection (ex. `W 52 St & 11 Ave`) or landmark (ex. `Yankee Ferry Terminal`).
    * `Penn Station Valet` is a special exception.
    * Storage depots (removed from this dataset) have `DEPOT` in their name.
* `incoming trips` &mdash; Number of trips whose endpoint is this station.
* `outgoing trips` &mdash; Number of trips whose starting point is this station.
* `all trips` &mdash; Number of trips that end or being here (`incoming trips` + `outgoing trips`)
* `kind` &mdash; `Active` if the station is in use, `Inactive` if it is down/out for maintenance, `Depot` if it is a
storage depot.
* `bikes outbound` &mdash; Number of bikes which start their day here and get ridden out.
* `outbound trips` &mdash; Number of trips taken by bikes which start their day here.
* `bikes inbound` &mdash; Number of bikes which end their day here.
* `inbound trips` &mdash; Number of trips taken by bikes which end their day here.
* `delta bikes` &mdash; Difference in number of bikes between start and end of the day (`bikes inbound` - `bikes
outbound`).
* `delta trips` &mdash; Difference in number of trips taken by bikes which start their day here (`incoming trips` +
`outgoing trips`).
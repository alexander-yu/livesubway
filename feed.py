import cPickle as pickle

from eventlet.greenthread import sleep, spawn

import requests
import simplejson as json

from API_KEYS import mta_key
from static import Edge, Segment, StopID, Stop, StopGraph, PrevStops  # noqa: F401

import gtfs_realtime_pb2 as gtfs

JSON_DIR = "static/json/"
PICKLE_DIR = ".cache/"
MTA_ENDPOINT = "http://datamine.mta.info/mta_esi.php?key={}&feed_id=1" \
    .format(mta_key)

current_feed = None

with open(PICKLE_DIR + "graph.pkl", "rb") as graph_f, \
        open(PICKLE_DIR + "prev_stops.pkl", "rb") as prev_stops_f, \
        open(JSON_DIR + "shapes.json", "r") as shapes_f, \
        open(JSON_DIR + "stops.json", "r") as stops_f:
    shapes = json.load(shapes_f)
    stops = json.load(stops_f)
    graph = pickle.load(graph_f)
    prev_stops = pickle.load(prev_stops_f)

# TODO: get nice way of retrieving remaining time until next stop
demos = [
    [
        {
            "path": [[-73.96411, 40.807722], [-73.958372, 40.815581]],
            "progress": 0.5,
            "remaining_time": 10
        },
        {
            "path": graph.get_path("118S", "119S", shapes),
            "progress": 0.3,
            "remaining_time": 15
        }
    ],
    [
        {
            "path": [[-73.96411, 40.807722], [-73.959874, 40.77362]],
            "progress": 0.5,
            "remaining_time": 10
        },
        {
            "path": [[-73.958372, 40.815581], [-73.987691, 40.755477]],
            "progress": 0.3,
            "remaining_time": 25
        },
    ],
    [
        {
            "path": [[-73.958372, 40.815581], [-73.987691, 40.755477]],
            "progress": 0.3,
            "remaining_time": 25
        },
        {
            "path": [[-73.992629, 40.730328], [-73.989951, 40.734673]],
            "progress": 0.3,
            "remaining_time": 15
        }
    ]
]


class VehiclePosition:
    def __init__(self, vehicle):
        self.next_stop = vehicle.stop_id
        self.prev_stop = prev_stops.get_prev_stop(vehicle)
        self.prev_trip_time = prev_stops.get_prev_trip_time(self.prev_stop,
                                                            vehicle)
        if self.prev_stop is not None:
            self.path = graph.get_path(self.prev_stop, self.next_stop, shapes)
        else:
            # Vehicle is at beginning of its trip (sitting in its stop); thus,
            # we simply assume that the vehicle remains stationary until next
            # feed
            station = graph.get_parent_station(self.next_stop)
            self.path = [stops[station]["coordinates"]]


def start_timer():
    return spawn(feed_timer)


def feed_timer():
    while True:
        global current_feed
        current_feed = spawn(get_feed).wait()
        sleep(30)


def get_feed():
    raw_gtfs = requests.get(MTA_ENDPOINT)
    feed = gtfs.FeedMessage()
    feed.ParseFromString(raw_gtfs.content)
    return feed


def get_vehicle_positions():
    feed = get_feed()
    for entity in feed.entity:
        pass

# testing API usage
# for entity in feed.entity:
#     if (entity.trip_update.trip.HasExtension(nyct.nyct_trip_descriptor)):
#         print entity.trip_update.trip.Extensions[nyct.nyct_trip_descriptor]

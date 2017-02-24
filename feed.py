import cPickle as pickle

from eventlet.greenthread import sleep, spawn

import requests
import simplejson as json

from API_KEYS import mta_key
from static import Edge, Segment, StopID, Stop, StopGraph, PrevStops  # noqa: F401

import gtfs_realtime_pb2 as gtfs

JSON_DIR = "map_files/"
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


class VehiclePosition:
    def __init__(self, vehicle, trip_update):
        next_stop = vehicle.stop_id
        prev_stop = prev_stops.get_prev_stop(vehicle)
        prev_trip_time = prev_stops.get_prev_trip_time(prev_stop,
                                                       vehicle)
        if prev_trip_time:
            self.remaining_time = int(trip_update.arrival.time -
                                      vehicle.timestamp)
            self.progress = max(1 - float(self.remaining_time) / prev_trip_time, 0)
        else:
            self.remaining_time = 0
            self.progress = 1

        if prev_stop is not None:
            self.path = graph.get_path(prev_stop, next_stop, shapes)
        else:
            # Vehicle is at beginning of its trip (sitting in its stop); thus,
            # we simply assume that the vehicle remains stationary until next
            # feed
            station = graph.get_parent_station(next_stop)
            self.path = [stops[station]["coordinates"]]

    def jsonify(self):
        return {
            "path": self.path,
            "progress": self.progress,
            "remaining_time": self.remaining_time
        }


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
    trip_updates = {}
    vehicle_positions = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            trip_id = trip_update.trip.trip_id
            update = trip_update.stop_time_update[0]
            trip_updates[trip_id] = update

        elif entity.HasField("vehicle"):
            vehicle = entity.vehicle
            trip_id = vehicle.trip.trip_id
            assert trip_id in trip_updates, entity
            trip_update = trip_updates

            # TODO: Take care of case where remaining_time < 30 seconds
            position_json = VehiclePosition(vehicle, trip_update[trip_id]) \
                .jsonify()
            vehicle_positions.append(position_json)

    return vehicle_positions

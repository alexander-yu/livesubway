import cPickle as pickle
import os

from argparse import ArgumentParser
from bisect import bisect_left
from collections import namedtuple
from datetime import date

import simplejson as json
import transitfeed

# TODO: Move this to a database, or make it more efficient in general

JSON_DIR = "map_files/"
PICKLE_DIR = ".cache/"
STATIC_TRANSIT_DIR = "transit_files/"

if not os.path.isdir(JSON_DIR):
    os.makedirs(JSON_DIR)
if not os.path.isdir(PICKLE_DIR):
    os.makedirs(PICKLE_DIR)

Segment = namedtuple('Segment', ['start', 'end'])
Edge = namedtuple('Edge', ['shape_id', 'start_index', 'end_index'])
StopID = namedtuple('StopID',
                    ['route', 'stop_id'])


class Coordinates(namedtuple('Coordinates', ['lon', 'lat'])):
    def __str__(self):
        return "({}, {})".format(self.lon, self.lat)

    def array(self):
        return [self.lon, self.lat]

WEEKDAY_SERVICE_CODE = "WKD"
SAT_SERVICE_CODE = "SAT"
SUN_SERVICE_CODE = "SUN"

# We group routes by their "color" designation into sets; the reason for this
# is that routes in a particular route group have overlap and cover overlapping
# stops, at least in Manhattan. Thus, often times for maintenace or other
# purposes, a train on one of these routes may be routed to another route in
# the same group, such as a 3 train running local, which would mean it would
# run a similar set of stops as a normal 1 train. However, this type of
# situation is currently not covered in the static data, so we group these
# routes up when looking at the live feed and determining what the previous
# stop of a subway car may be from the static data.
ROUTE_GROUPS = [
    set(["1", "2", "3"]),
    set(["4", "5", "6"]),
    set(["A", "C", "E"]),
    set(["B", "D", "F", "M"]),
    set(["J", "Z"]),
    set(["N", "Q", "R", "W"])
]

ROUTE_GROUP_MAPPING = {
    route: route_group
    for route_group in ROUTE_GROUPS
    for route in route_group
}

# Currently the new South Ferry station is closed due to the effects of
# Hurricane Sandy, forcing the old South Ferry station to be recommissioned
# repairs are done. However, while this is accurate in shapes.txt, the new stop
# is still being used in stops.txt, and so we replace occurrences of the new
# South Ferry stop with the old one until the static information is updated.
OLD_SOUTH_FERRY = Coordinates(-74.013664, 40.702068)
NEW_SOUTH_FERRY = Coordinates(-74.013205, 40.701411)

# shapes.txt currently has a hole containing the York St. stop for some reason,
# so currently while we still refer to the York St. stop as a real stop,
# currently for purposes of convenience, we store the corresponding edge as
# the closest point to the York St. stop, and simply use this as the other
# endpoint. Moreover, since the York St. stop is not in shapes.txt, we also
# provide relevant information for the stop ID/shape for creating the edges.
#
# We may be able to have two approximate points depending on the
# direction of the segment, but not entirely sure if this is necessary.
# Hopefully this gets fixed in a future iteration of the static transit
# information.
YORK_STREET = Coordinates(-73.986751, 40.701397)
YORK_STREET_APPROX = Coordinates(-73.986885, 40.699743)
YORK_STREET_ID = "F18"
YORK_STREET_SHAPE = "F..N68R"

# The script currently skips paths that go along the Second Avenue Subway Line,
# as these are part of the new N/Q (and soon to be T) lines that open up in
# January 2017. While this data is provided as part of stops/stop_times, the
# shapes are not provided in shapes.txt, so we skip these paths until the
# static information is eventually updated (hopefully the next iteration once
# the lines begin operation).
SECOND_AVE_PATHS = set(["N..N63R", "N..N67R", "N..S16R", "Q..N16R", "Q..N19R",
                        "Q..S16R", "Q..S19R"])


class Stop:
    """ Stop class.

    Used primarily to store information concerning stops that may
    precede a particular stop, and the trip paths that correspond
    to a pair of this stop and a previous stop possibility.
    """
    def __init__(self):
        """ Constructor. """
        self.trip_paths_by_prev_stop = {}
        self.prev_stops_by_stop_sequence = {}
        self.prev_stops = set()

    def add_prev_stop(self, stop_sequence, prev_stop, trip_path):
        """ Adds a previous stop possibility along a trip path.

        Arguments
        ---------
        stop_sequence: int
            Stop sequence of stop along a trip path
        prev_stop: str
            Stop ID of preceding stop along a trip path
        trip_path: str
            Trip path ID containing stop
        """
        if prev_stop not in self.trip_paths_by_prev_stop:
            self.prev_stops.add(prev_stop)
            self.trip_paths_by_prev_stop[prev_stop] = set([trip_path])
        else:
            self.trip_paths_by_prev_stop[prev_stop].add(trip_path)

        if stop_sequence not in self.prev_stops_by_stop_sequence:
            self.prev_stops_by_stop_sequence[stop_sequence] = set([prev_stop])
        else:
            self.prev_stops_by_stop_sequence[stop_sequence].add(prev_stop)


class PrevStops:
    """ PrevStops class.

    Used primarily to store static information concerning stops
    in order to retrieve the preceding stop for a given subway car.
    This information is needed in order to render the duration of the path
    of the subway car.
    """
    def __init__(self, schedule):
        """ Constructor.

        Arguments
        ---------
        schedule: transitfeed.Schedule
            Schedule object
        """
        self._all_prev_stops = PrevStops._get_all_prev_stops(schedule)
        self._ambiguous_trips = \
            PrevStops._get_ambiguous_trip_paths(self._all_prev_stops)
        self._ambiguous_stop_sequences = \
            PrevStops._get_ambiguous_stop_sequences(self._ambiguous_trips,
                                                    schedule)

    @staticmethod
    def _get_service_code(trip):
        """ Returns corresponding service code of a trip.

        Arguments
        ---------
        trip: transit_realtime.TripDescriptor
            GTFS realtime TripDescriptor object (protobuf)

        Returns
        -------
        str
            service code of trip
        """
        start_date = trip.start_date
        year, month, day = int(start_date[:4]), int(start_date[4:6]), \
            int(start_date[6:])
        day_of_week = date(year, month, day).isoweekday()

        if day_of_week <= 5:
            service_code = WEEKDAY_SERVICE_CODE
        elif day_of_week == 6:
            service_code = SAT_SERVICE_CODE
        else:
            service_code = SUN_SERVICE_CODE

        return service_code

    @staticmethod
    def _get_all_prev_stops(schedule):
        """ Returns map of StopID -> Stop object for every possible
        StopID in the static transit data.

        Arguments
        ---------
        schedule: transitfeed.Schedule
            Schedule object

        Returns
        -------
        dict[StopID -> Stop]
            Map of StopID -> Stop object
        """
        all_prev_stops = {}
        trip_paths = set()

        for trip_object in schedule.GetTripList():
            trip_path = trip_object.trip_id.rsplit("_", 1)[1]
            route = trip_object.route_id

            # No need to duplicate work over trip paths already seen,
            # since a trip path uniquely defines a sequence of stops
            if trip_path not in trip_paths:
                stop_times = trip_object.GetStopTimes()
                for stop_time in stop_times:
                    stop_id = stop_time.stop.stop_id
                    stop_sequence = stop_time.stop_sequence
                    stop_id = StopID(route, stop_id)

                    if stop_id not in all_prev_stops:
                        all_prev_stops[stop_id] = Stop()

                    stop = all_prev_stops[stop_id]

                    # We ignore the case where the stop is at the beginning,
                    # since clearly there is no previous stop
                    if stop_sequence > 1:
                        # Subtract 2 because we need to access previous stop,
                        # and stop_sequence is 1-indexed, rather than 0-indexed
                        prev_stop = stop_times[stop_sequence - 2].stop.stop_id
                        stop.add_prev_stop(stop_sequence, prev_stop, trip_path)

                trip_paths.add(trip_path)

        return all_prev_stops

    @staticmethod
    def _get_ambiguous_trip_paths(all_prev_stops):
        """ Returns map of trip path -> set of pairs of StopID + previous
        stops that are ambiguous and contained in that trip path; i.e. have
        multiple trip paths that contain the info of that StopID and that
        particular previous stop as the preceding stop.

        To clarify, when we attempt to figure out from the live MTA feed what
        the stop a particular subway car is coming from, we're only guaranteed
        the origin time of the trip, as well as the start date, route number,
        direction, and any future stop sequences along the planned trip.

        However, there are a select few cases where this is not enough
        information. Usually this is due to alternate trip paths that depend on
        the time of day/day of week.

        This map will thus keep track of the trip paths that are a part of
        these ambiguous cases; the values will be the set of pairs of StopIDs
        and the previous possibility for that StopID that lies on that
        particular trip path (we also store the previous stop to avoid an extra
        call to generate the list of stops for that trip path), and the keys
        are all such trip paths that do form an ambiguous scenario. This way,
        we can work with all ambiguous cases in one pass of the trip list.

        Arguments
        ---------
        all_prev_stops: dict[StopID -> Stop]
            Map of StopID -> Stop object

        Returns
        -------
        dict[str -> set[tuple[StopID, str]]]
            Map of trip path -> set of pairs of StopID + previous
            stops that are ambiguous and are contained in that trip path
        """
        ambiguous_trip_paths = {}

        for stop_id, stop in \
                all_prev_stops.iteritems():
            # If there is more than one possible previous stop,
            # we have an ambiguous case
            if len(stop.prev_stops) > 1:
                for prev_stop in stop.trip_paths_by_prev_stop:
                    for trip_path in stop.trip_paths_by_prev_stop[prev_stop]:
                        if trip_path not in ambiguous_trip_paths:
                            ambiguous_trip_paths[trip_path] = set()

                        ambiguous_trip_paths[trip_path] \
                            .add((stop_id, prev_stop))

        return ambiguous_trip_paths

    @staticmethod
    def _get_ambiguous_stop_sequences(ambiguous_trip_paths, schedule):
        """ Returns map of StopID -> map of possible previous
        stops for that particular StopID over all trips containing the
        info of the StopID, keyed by service code and sorted by origin time
        of the corresponding trip.

        This map is only used when there is ambiguity, and the StopID and stop
        sequence are not enough to determine the previous stop.

        In these cases, an approximate solution is used. We store a list of the
        possible previous stop possibilities for every trip (including distinct
        origin times), and these lists are sorted by origin time. Then in order
        to find the most likely previous stop given a particular StopID,
        we simply find the corresponding previous stop (for a particular trip)
        that has the origin time closest to the origin time of the live trip
        and matching the same service code (i.e. weekday, Saturday, or Sunday).

        Arguments
        ---------
        ambiguous_trip_paths: dict[str -> set[tuple[StopID, str]]]
            Map of trip path -> set of pairs of StopID + previous stop
            possibilities such that the trip path contains the info of the
            StopID and the previous stop is the preceding stop of the StopID
            on the trip path
        schedule: transitfeed.Schedule
            Schedule object

        Returns
        -------
        dict[StopID -> dict[str -> set[tuple[str, str]]]]
            Map of StopID -> map of service code (i.e. WKD, SAT, SUN) ->
            set of tuples of (origin time, previous stop)
        """
        ambiguous_stop_sequences = {}

        # Populate pairs of origin times + corresponding previous stops for
        # each possible trip path for a given StopID + service code
        for trip_object in schedule.GetTripList():
            service_code = trip_object.service_id[-3:]
            origin_time, trip_path = trip_object.trip_id.split("_")[1:]

            if trip_path in ambiguous_trip_paths:
                for stop_id, prev_stop in ambiguous_trip_paths[trip_path]:
                    if stop_id not in ambiguous_stop_sequences:
                        ambiguous_stop_sequences[stop_id] = {}

                    if service_code not in \
                            ambiguous_stop_sequences[stop_id]:
                        ambiguous_stop_sequences[stop_id][service_code] \
                            = []

                    ambiguous_stop_sequences[stop_id][service_code] \
                        .append((origin_time, prev_stop))

        # Then sort the populated pairs and split the pairs into individual
        # lists
        for prev_stops_by_service_code in ambiguous_stop_sequences.values():
            for prev_stops in prev_stops_by_service_code.values():
                # Sort by origin time
                prev_stops.sort(key=lambda x: int(x[0]))

            for service_code in prev_stops_by_service_code:
                # Split sorted pairs into sorted lists of origin times and
                # corresponding previous stop possibilities
                prev_stops = prev_stops_by_service_code[service_code]
                sorted_origin_times, sorted_prev_stops = zip(*prev_stops)
                prev_stops_by_service_code[service_code] = {
                    "origin_times": sorted_origin_times,
                    "prev_stops": sorted_prev_stops
                }

        return ambiguous_stop_sequences

    def get_prev_stop(self, vehicle):
        """ Returns a possible previous stop for a given trip
        and stop.

        Arguments
        ---------
        vehicle: transit_realtime.VehiclePosition
            GTFS realtime VehiclePosition object (protobuf)

        Returns
        -------
        str
            stop ID of possible previous stop
        """
        trip = vehicle.trip
        route = trip.trip_id.split("_")[1].split(".")[0]
        stop_id = StopID(
            route,
            vehicle.stop_id
        )
        stop_sequence = vehicle.current_stop_sequence

        if stop_id in self._all_prev_stops:
            stop = self._all_prev_stops[stop_id]
        # If the stop ID is not present, perhaps the car has switched
        # to another route; see the comments for the ROUTE_GROUPS constant
        # at the top. Unfortunately this isn't a perfect method, since it
        # does not necessarily correctly determine what the actual route it
        # switched to, but this information is not necessarily known just
        # from the vehicle itself (one needs to look either at live trip
        # updates on the feed or for live service alerts). Thus we simply
        # find the first match and break.
        else:
            alternative_found = False
            for route in ROUTE_GROUP_MAPPING[route]:
                stop_id = StopID(route, vehicle.stop_id)
                if stop_id in self._all_prev_stops:
                    stop = self._all_prev_stops[stop_id]
                    alternative_found = True
                    break

            # If a stop ID is still not found, we search all possible routes;
            # this may happen in case of service changes due to maintenance
            # or other problems in the subway. Unfortunately, similarly to
            # above, this is not a perfect method, since we simply find the
            # first match and break.
            if not alternative_found:
                for route_group in ROUTE_GROUPS:
                    for route in route_group:
                        stop_id = StopID(route, vehicle.stop_id)
                        if stop_id in self._all_prev_stops:
                            stop = self._all_prev_stops[stop_id]
                            break

        # If vehicle is at the beginning of its trip, there is
        # obviously no previous stop
        if stop_sequence == 1:
            return None
        # If there is only a unique previous stop among all trip
        # paths, simply return that stop
        elif len(stop.prev_stops) == 1:
            return next(iter(stop.prev_stops))
        else:
            # If there is a unique previous stop corresponding also
            # to the stop sequence number, return that previous stop
            if stop_sequence in stop.prev_stops_by_stop_sequence:
                prev_stops = stop.prev_stops_by_stop_sequence[stop_sequence]
                if len(prev_stops) == 1:
                    return next(iter(prev_stops))

            # Otherwise, if the stop sequence number with the StopID
            # does not guarantee a unique previous stop, or the stop
            # sequence number does not match with a known number,
            # attempt to guess a likely possibility by origin time
            return self._get_prev_stop_by_origin_time(trip, stop_id)

    def _get_prev_stop_by_origin_time(self, trip, stop_id):
        """ Returns a possible previous stop for a given trip
        and stop that is closest in origin time.

        Given the StopID and the trip's service code and origin
        time, this method attempts to find a corresponding trip
        in the static data that matches, or at least as closely
        as possible in terms of origin time. The rationale for this
        is based off of the assumption that the corresponding train
        on the same route and day is most likely going to match with
        the closest scheduled train on that route and day.

        There is a possibility of multiple previous stops for
        the given information (as there may be multiple trips
        on the same day and route starting at the same time),
        and currently there is no real tiebreaker method, other
        than the rightmost tied element. That's mainly because
        given that this method is only used in the last case,
        this means that given the route, stop, stop sequence,
        service code, and origin time (and even direction), it
        was not enough to decide the actual previous stop. But
        this is all of the information that is guaranteed by
        a particular Vehicle in the feed.

        One can use future stops to match with possible trips
        or partial string matching in case an actual trip path ID
        is given at a certain point, but not entirely sure if the
        effort is worth it; out of the few observations of the live
        feed so far, this case isn't encountered.

        TODO: Do testing/observations to see if this edge case
        ever occurs

        Arguments
        ---------
        trip: transit_realtime.TripDescriptor
            GTFS realtime TripDescriptor object (protobuf)
        stop_id: StopID
            StopID object

        Returns
        -------
        str
            stop ID of possible previous stop
        """
        service_code = PrevStops._get_service_code(trip)
        sorted_prev_stop_pairs = \
            self._ambiguous_stop_sequences[stop_id][service_code]
        origin_time = trip.trip_id.split("_")[0]
        sorted_origin_times = sorted_prev_stop_pairs["origin_times"]

        # We do this in case the origin time of the vehicle is later or
        # earlier than all stored origin times for a particular stop
        # sequence/route/service code. Instead of cycling around, we simply
        # let left = right in these edge cases, as otherwise we would have
        # to take into account the next day, possibly a different service
        # code, but this then contradicts the specified start date of the
        # vehicle.
        right = min(
            bisect_left(sorted_origin_times, origin_time),
            len(sorted_origin_times) - 1
        )
        left = max(0, right - 1)

        closest_index = min([
            (abs(int(origin_time) -
                 int(sorted_prev_stop_pairs["origin_times"][candidate])),
                candidate)
            for candidate in [left, right]
        ])[1]

        return sorted_prev_stop_pairs["prev_stops"][closest_index]


class StopGraph:
    """ StopGraph class.

    Used primarily to store static information concerning stops
    in order to retrieve the sequence of points between adjacent stops
    on a particular trip. This information is needed in order to render the
    frames of the path of the subway car.
    """
    def __init__(self, schedule):
        """ Constructor.

        Arguments
        ---------
        schedule: transitfeed.Schedule
            Schedule object
        """
        shape_indices = StopGraph._get_shape_indices(schedule)
        stop_shapes = StopGraph._get_stop_shapes(schedule)
        self._edges = StopGraph._get_edges(schedule, stop_shapes,
                                           shape_indices)

    @staticmethod
    def _get_stop_coords(stop_object):
        """ Return coordinates of a transitfeed.Stop object.

        Arguments
        ---------
        stop_object: transitfeed.Stop
            transitfeed.Stop object

        Returns
        -------
        Coordinates
            Coordinates of transitfeed.Stop object
        """
        coordinates = Coordinates(stop_object.stop_lon,
                                  stop_object.stop_lat)
        # See top of script for an explanation of why the
        # South Ferry stop is handled differently.
        if coordinates == NEW_SOUTH_FERRY:
            return OLD_SOUTH_FERRY
        else:
            return coordinates

    @staticmethod
    def _get_shape_indices(schedule):
        """ Return map of point indices for each shape.

        This is used for forming the edges between stops,
        so that each edge can find the corresponding indices
        of two stops along a shape and store these indices as the
        boundary indices of the edge (then when sending the GPS
        coordinates to the client code, we can simply use an array
        slice on these indices from the shape's point sequence).

        Arguments
        ---------
        schedule: transitfeed.Schedule
            Schedule object

        Returns
        -------
        dict[str -> dict[Coordinates -> int]]
            Map of shape ID -> map of point -> index of point
        """
        shape_indices = {}

        for shape_object in schedule.GetShapeList():
            shape_id = shape_object.shape_id
            shape_indices[shape_id] = {}

            for i in xrange(len(shape_object.points)):
                point = shape_object.points[i]
                # We reverse the coordinates, as GTFS stores coordinates as
                # (lat, lon) while Mapbox stores coordinates as (lon, lat).
                coordinates = Coordinates(point[1], point[0])
                shape_indices[shape_id][coordinates] = i

        return shape_indices

    @staticmethod
    def _get_stop_shapes(schedule):
        """ Return map of stop ID -> set of shapes containing each stop.

        Arguments
        ---------
        schedule: transitfeed.Schedule
            Schedule object

        Returns
        -------
        dict[str -> set[str]]
            Map of stop ID -> set of shape IDs containing that stop's
            coordinates
        """
        stop_coords = {}
        stop_shapes = {}

        for stop_object in schedule.GetStopList():
            # Only consider stops that are parent stations to avoid redundancy
            if stop_object.location_type == 1:
                stop_id = stop_object.stop_id
                coordinates = StopGraph._get_stop_coords(stop_object)
                if coordinates in stop_coords:
                    stop_coords[coordinates].append(stop_id)
                else:
                    stop_coords[coordinates] = [stop_id]

        for shape_object in schedule.GetShapeList():
            shape_id = shape_object.shape_id

            # For each point in the shape, check if it coordinates to at least
            # one stop. If so, for each matching stop add this particular shape
            # to the set of shapes containing that stop.
            for point in shape_object.points:
                coordinates = Coordinates(point[1], point[0])
                if coordinates in stop_coords:
                    stop_ids = stop_coords[coordinates]
                    for stop_id in stop_ids:
                        if stop_id not in stop_shapes:
                            stop_shapes[stop_id] = set([shape_id])
                        else:
                            stop_shapes[stop_id].add(shape_id)

        return stop_shapes

    @staticmethod
    def _get_stop_edge(schedule, segment, stop_shapes, shape_indices):
        """ Return an edge of points between stops.

        The Edge that is constructed contains a shape ID for a shape that
        contains both the start and end stop, as well as the corresponding
        indices of those stops in the sequence of points for that shape.

        Arguments
        ---------
        schedule: transitfeed.Schedule
            Schedule object
        segment: Segment
            Segment of start/end transitfeed.Stop objects
        stop_shapes: dict[str -> set[str]]
            Map of stop ID -> set of shape IDs containing that stop's
            coordinates
        shape_indices: dict[str -> dict[str -> int]]
            Map of shape ID -> map of point -> index of point

        Returns
        -------
        Edge
            Edge between the two stops
        """
        start = segment.start
        end = segment.end

        start_coords = StopGraph._get_stop_coords(start)
        end_coords = StopGraph._get_stop_coords(end)

        start_station = schedule.GetStop(start.stop_id) \
            .parent_station
        end_station = schedule.GetStop(end.stop_id).parent_station

        # See comments above declaration of these constants at the top of the
        # script for an explanation of why York St. cases are handled
        # differently.
        if start_station == YORK_STREET_ID:
            shape_id = YORK_STREET_SHAPE
            start_index = \
                shape_indices[shape_id][YORK_STREET_APPROX]
            end_index = shape_indices[shape_id][end_coords]
        elif end_station == YORK_STREET_ID:
            shape_id = YORK_STREET_SHAPE
            start_index = \
                shape_indices[shape_id][start_coords]
            end_index = \
                shape_indices[shape_id][YORK_STREET_APPROX]
        else:
            # We assume that there is a unique path between any two adjacent
            # stops on the entire map for each trip, or if there isn't, the
            # paths are very similar in length/shape, which appears to be the
            # case, so the choice of shape doesn't matter, as long as it
            # contains both stops.
            common_shapes = stop_shapes[start_station] \
                .intersection(stop_shapes[end_station])
            shape_id = common_shapes.pop()
            start_index = shape_indices[shape_id][start_coords]
            end_index = shape_indices[shape_id][end_coords]

        return Edge(shape_id, start_index, end_index)

    @staticmethod
    def _get_edges(schedule, stop_shapes, shape_indices):
        """ Returns a map information about the edges of points between
        adjacent stops along paths of the subway lines.

        Edges are mapped by the endpoints, with the following structure:
        {
            Segment(start_station_id, end_station_id): Edge(
                shape_id: shape ID containing start/end stops,
                start_index: index of start stop in shape,
                end_index: index of end stop in shape
            )
        }

        Arguments
        ---------
        schedule: transitfeed.Schedule
            Schedule object
        stop_shapes: dict[str -> set[str]]
            Map of stop ID -> set of shape IDs containing that stop's
            coordinates
        shape_indices: dict[str -> dict[str -> int]]
            Map of shape ID -> map of point -> index of point

        Returns
        -------
        dict[Segment[str, str] -> Edge(str, int, int)]
            Map of Segment of start/stop stations -> Edge representing
            sequence of points along Segment
        """
        edges = {}
        for trip_object in schedule.GetTripList():
            # For an explanation of why trip paths along 2nd Avenue
            # are currently skipped, see the top of the script.
            trip_path = trip_object.trip_id.rsplit("_", 1)[1]
            if trip_path in SECOND_AVE_PATHS:
                continue

            stops = trip_object.GetPattern()
            for i in xrange(len(stops) - 1):
                start = stops[i]
                end = stops[i + 1]

                start_station = schedule.GetStop(start.stop_id) \
                    .parent_station
                end_station = schedule.GetStop(end.stop_id) \
                    .parent_station

                # If this edge (up to orientation) has not been seen before,
                # add to map.
                if Segment(start_station, end_station) not in edges and \
                        Segment(end_station, start_station) not in edges:
                    edges[Segment(start_station, end_station)] = \
                        StopGraph._get_stop_edge(schedule,
                                                 Segment(start, end),
                                                 stop_shapes,
                                                 shape_indices)

        return edges

    def get_path(self, start, end, shapes):
        """ Returns sequence of points between two stops.

        The two stops need to be adjacent stops on any particular
        trip (local or express).

        Moreover, only one edge for each segment of stop endpoints is stored;
        thus, in order to return the correct sequence of points, we must keep
        track of two things. One, the relative orientation of the requested
        stops with the segment of stops stored, and two, the actual
        orientation of the stored edge with the shape. In particular, the
        stored edge may represent a path that goes northbound (since edges are
        represented as a start/end structure and are thus "directed"), but the
        shape that is being used for this edge may actually be southbound.

        Thus, we store these orientations as +/-1, and simply compose
        the orientations to see whether or not a reverse slice is needed.

        Arguments
        ---------
        start: str
            Station ID of start stop (must be a parent station)
        end: str
            Station ID of end stop (must be a parent station)
        shapes: dict[str -> dict[str -> list[[float, float]]]]
            Map of the form shape ID -> map of "points" -> list of coordinates
            in the form [lon, lat]

        Returns
        -------
        list[[float, float]]
            List of coordinates in the form [lon, lat]
        """
        if Segment(start, end) in self._edges:
            edge = self._edges[Segment(start, end)]
            relative_orientation = 1
        else:
            edge = self._edges[Segment(end, start)]
            relative_orientation = -1

        shape_id = edge.shape_id
        start_index, end_index = sorted((edge.start_index,
                                        edge.end_index))
        shape_orientation = 1 if start_index == edge.start_index else -1
        points = shapes[shape_id]["points"]

        # Regular slice suffices
        if relative_orientation * shape_orientation == 1:
            return points[start_index:end_index + 1]
        # Reverse slice is needed
        else:
            if start_index == 0:
                return points[end_index::-1]
            else:
                return points[end_index:start_index - 1:-1]


def parse_shapes(schedule):
    """ Writes shapes.json.

    This JSON file is sent to the client code in order to render
    the actual subway lines on the map. It is also used to retrieve
    sequences of points used to animate the paths of the subway cars
    along the subway lines.

    Writes a JSON file of the following format:
    {
        shape_id: {
            color: route color for shape,
            sequence: number of points in shape,
            points: [[lon, lat], ...]
        }
    }

    Arguments
    ---------
    schedule: transitfeed.Schedule
        Schedule object
    """
    with open(JSON_DIR + "shapes.json", "w") as shapes_f:
        shapes = {}

        for shape_object in schedule.GetShapeList():
            shape_id = shape_object.shape_id
            shape = shapes[shape_id] = {}

            shape["sequence"] = shape_object.sequence[-1]
            shape["points"] = []

            color = ''
            for route in schedule.GetRouteList():
                if shape_id[0] == route.route_id[0]:
                    color = "#" + route.route_color

            shape["color"] = color

            for point in shape_object.points:
                # We reverse the coordinates, as GTFS stores coordinates as
                # (lat, lon) while Mapbox stores coordinates as (lon, lat).
                # Moreover, we use an array here as opposed to the Coordinates
                # class for ease at the cost of readability, as the points in
                # shapes.json will be passed to Mapbox, which only handles GPS
                # coordinates in array format.
                coordinates = [point[1], point[0]]
                shape["points"].append(coordinates)

        shapes_f.write(json.dumps(shapes))
        print "shapes.json written."


def parse_stops(schedule):
    """ Writes stops.json.

    This JSON file is sent to the client code to render the stops on the map.

    Writes a JSON file of the following format:
    {
        stop_id: {
            coordinates: {
                lat: latitude,
                lon: longitude
            },
            name: name
        }
    }

    Arguments
    ---------
    schedule: transitfeed.Schedule
        Schedule object
    """
    with open(JSON_DIR + "stops.json", "w") as stops_f:
        stops = {}

        for stop_object in schedule.GetStopList():
            # Only consider stops that are parent stations to avoid redundancy
            if stop_object.location_type == 1:
                stop_id = stop_object.stop_id
                stop = stops[stop_id] = {}

                coordinates = Coordinates(stop_object.stop_lat,
                                          stop_object.stop_lon)
                if coordinates == NEW_SOUTH_FERRY:
                    coordinates = OLD_SOUTH_FERRY

                stop["coordinates"] = coordinates.array()
                stop["name"] = stop_object.stop_name

        stops_f.write(json.dumps(stops))
        print "stops.json written."


def parse_graph(schedule):
    """ Writes graph.pkl.

    Seralizes a StopGraph object. This serialized object is used to retrieve
    sequences of points used to animate the paths of the subway cars along the
    subway lines.

    Arguments
    ---------
    schedule: transitfeed.Schedule
        Schedule object
    """
    with open(PICKLE_DIR + "graph.pkl", "wb") as graph_f:
        pickle.dump(StopGraph(schedule), graph_f, pickle.HIGHEST_PROTOCOL)
        print "graph.pkl written."


def parse_prev_stops(schedule):
    """ Writes prev_stops.pkl.

    Serializes a PrevStops object. This serialized object is used to retrieve
    the previous stop that a subway car in the live feed is coming from using
    the given information in the live feed.

    Arguments
    ---------
    schedule: transitfeed.Schedule
        Schedule object
    """
    with open(PICKLE_DIR + "prev_stops.pkl", "wb") as prev_stops_f:
        pickle.dump(PrevStops(schedule), prev_stops_f, pickle.HIGHEST_PROTOCOL)
        print "prev_stops.pkl written."


def get_parser():
    """ Returns argument parser. """
    parser = ArgumentParser(
        description="A script to write files with " +
        "needed static transit data."
    )
    parser.add_argument(
        "-g",
        "--graph",
        action="store_true",
        default=False,
        help="Flag to enable creation of graph.pkl"
    )
    parser.add_argument(
        "-o",
        "--stops",
        action="store_true",
        default=False,
        help="Flag to enable creation of stops.json"
    )
    parser.add_argument(
        "-a",
        "--shapes",
        action="store_true",
        default=False,
        help="Flag to enable creation of shapes.json"
    )
    parser.add_argument(
        "-p",
        "--prev_stops",
        action="store_true",
        default=False,
        help="Flag to enable creation of prev_stops.pkl"
    )

    return parser


def write_static_files(args):
    """ Writes the various files/objects storing useful static information.

    Through this method, one can selectively choose which files/objects
    write, in case there are a select number missing/need updates, without
    having to redo everything.

    Arguments
    ---------
    args: argparse.Namespace
        Arguments
    """
    PARSE_FUNCTIONS = {
        "graph": parse_graph,
        "stops": parse_stops,
        "shapes": parse_shapes,
        "prev_stops": parse_prev_stops
    }

    print "Loading static schedule information..."
    loader = transitfeed.Loader(STATIC_TRANSIT_DIR)
    schedule = loader.Load()
    print "Done. Writing to file(s)..."

    for file, parse_function in PARSE_FUNCTIONS.iteritems():
        if not getattr(args, file):
            print "Skipping {}.".format(file)
        else:
            print "Writing {}...".format(file)
            parse_function(schedule)

    print "File(s) written."


if __name__ == "__main__":
    write_static_files(get_parser().parse_args())

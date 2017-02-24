from eventlet import monkey_patch
from flask import Flask, json, jsonify, render_template
from flask_socketio import SocketIO

from API_KEYS import mapbox_key

from static import Edge, Segment, StopID, Stop, StopGraph, PrevStops  # noqa: F401
import feed

monkey_patch()

JSON_DIR = "map_files/"

app = Flask(__name__)
socketio = SocketIO(app)
feed_event = None

with open(JSON_DIR + "shapes.json", "r") as shapes_f, \
        open(JSON_DIR + "stops.json", "r") as stops_f, \
        open(JSON_DIR + "routes.json", "r") as routes_f, \
        open(JSON_DIR + "colors.json", "r") as colors_f:
    shapes = json.load(shapes_f)
    stops = json.load(stops_f)
    routes = json.load(routes_f)
    colors = json.load(colors_f)


@app.route('/')
def index():
    return render_template("index.html", mapbox_key=mapbox_key,
                           subway_routes=shapes.keys(),
                           route_colors=colors)


@app.route('/map_json/<route>')
def map_json(route):
    # Documentation for shapes.json:
    # shape_id: {
    #      sequence: number of points,
    #      color: route color,
    #      points: [[lon, lat],...,]
    # }
    return jsonify(shapes[route])


@app.route('/map_geojson')
def map_geojson():
    # Documentation for shapes.json:
    # shape_id: {
    #      sequence: number of points,
    #      color: route color,
    #      points: [[lon, lat],...,]
    # }
    return jsonify(routes)


@app.route('/stops_json')
def stops_json():
    # Documentation for stops.json:
    # stop_id: {
    #      coordinates: {
    #          lat: latitude,
    #          lon: longitude
    #      },
    #      name: name
    # }
    return jsonify(stops)


@socketio.on('get_feed')
def subway_cars():
    global feed_event
    if feed_event is None:
        feed_event = socketio.start_background_task(target=subway_cars_timer)


def subway_cars_timer():
    while True:
        vehicle_positions = feed.get_vehicle_positions()
        socketio.emit('feed', vehicle_positions)
        socketio.sleep(30)


if __name__ == "__main__":
    feed_thread = feed.start_timer()

    try:
        socketio.run(app, host="0.0.0.0", debug=True)
    finally:
        feed_thread.cancel()

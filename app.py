from eventlet import monkey_patch
from flask import Flask, json, jsonify, render_template
from flask_socketio import SocketIO

from API_KEYS import mapbox_key

import feed

monkey_patch()

JSON_DIR = "static/json/"

app = Flask(__name__)
socketio = SocketIO(app)
feed_event = None
with open(JSON_DIR + "shapes.json", "r") as shapes_f, \
        open(JSON_DIR + "stops.json", "r") as stops_f:
    shapes = json.load(shapes_f)
    stops = json.load(stops_f)


@app.route('/')
def index():
    return render_template("index.html", mapbox_key=mapbox_key)


@app.route('/map_json')
def map_json():
    # Documentation for shapes.json:
    # shape_id: {
    #      sequence: number of points,
    #      color: route color,
    #      points: [[lon, lat],...,]
    # }
    return jsonify(shapes)


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
    counter = 0
    while True:
        demo_emit = feed.demos[counter % len(feed.demos)]
        socketio.emit('feed', demo_emit)
        counter += 1
        socketio.sleep(30)


if __name__ == "__main__":
    feed_thread = feed.start_timer()

    try:
        socketio.run(app, debug=True)
    finally:
        feed_thread.cancel()

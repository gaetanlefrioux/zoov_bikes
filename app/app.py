from flask import Flask, request, json, render_template
from datetime import datetime, timedelta
import pytz
from utils import select_to_json

app = Flask(__name__)

@app.route("/free_bikes")
def free_bikes_template():
     return render_template('free_bikes.html', title='Free Zoov bikes')

@app.route("/api/free_bikes")
def free_bikes():
    current_time = datetime.now().astimezone(pytz.utc)
    lat = request.args.get("lat", default=48.731243, type=float)
    lon = request.args.get("lon", default=2.171251, type=float)
    free_since = request.args.get("free_since", default=24*60, type=float)
    min_date = current_time - timedelta(minutes=free_since)
    max_distance = request.args.get("max_distance", default=500000, type=int) / 1e3

    results = select_to_json("""
        select bike_id, lat, lon, vehicle_type_id, current_range_meters,
            calculate_distance(%s, %s, lat, lon, 'K')*1000 as distance,
            free_state_start
        from bikes
        where free_state_start > %s
        and is_free = TRUE
        and is_reserved = FALSE
        and is_disabled = FALSE
        and calculate_distance(%s, %s, lat, lon, 'K') < %s
        order by calculate_distance(%s, %s, lat, lon, 'K') asc;
    """, (lat, lon, min_date, lat, lon, max_distance, lat, lon))

    return app.response_class(
        response=json.dumps(results),
        status=200,
        mimetype='application/json'
    )

if __name__ == '__main__':
    app.run(host="0.0.0.0")

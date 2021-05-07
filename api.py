import flask
from connect import connect
from adtk.detector import OutlierDetector
from sklearn.neighbors import LocalOutlierFactor
from postgresql_to_pd import postgresql_to_pd
import pandas as pd
from adtk.data import validate_series
import datetime
import math


app = flask.Flask(__name__)
app.config["DEBUG"] = True

symptoms = ["temperature", "bloodOxygen", "heartRate", "cough"]

conn = connect()


@app.route('/isOutlier', methods=['POST'])
def is_outlier():
    if not flask.request.json or not 'symptom' in flask.request.json or not 'value' in flask.request.json or not 'ts' in flask.request.json or not 'uid' in flask.request.json:
        flask.abort(400)
    symptom = flask.request.json["symptom"]
    val = flask.request.json["value"]
    ts = flask.request.json["ts"]
    uid = flask.request.json["uid"]
    #date_time_obj = datetime.datetime.fromisoformat(ts)
    hour_before = datetime.datetime.now() - datetime.timedelta(hours=1)
    print(hour_before)
    if not (symptom in symptoms):
        return flask.jsonify({'message': f'Invalid Symptom! Accepted symptoms are {symptoms}'}), 400

    select_query_1_hour = f"select timestamp,\"{symptom}\" from symptom where {symptom} is not null and \"userId\" = '{int(uid)}' and timestamp > '{hour_before}' order by timestamp desc"
    #select_query = f"select timestamp,\"{symptom}\" from symptom where {symptom} is not null and \"userId\" = '{int(uid)}' order by timestamp desc"
    column_names = ["timestamp", symptom]
    df = postgresql_to_pd(conn, select_query_1_hour, column_names)
    print(df)
    if len(df) == 0:
        return flask.jsonify({'isOutlier': str(False), 'noData': str(True)}), 200
    df = df.append({
        f"{symptom}": val,
        "timestamp": ts
    }, ignore_index=True)
    df["DateTime"] = pd.to_datetime(df["timestamp"])
    df = df.set_index(pd.DatetimeIndex(df["DateTime"]))
    df = df.drop(["timestamp", "DateTime"], axis=1)
    validate_series(df)
    outlier_detector = OutlierDetector(LocalOutlierFactor(contamination=0.05,n_neighbors=int(round(len(df)/2))))
    anomalies = outlier_detector.fit_detect(df)
    return flask.jsonify({'outlier': str(anomalies.iat[-1])}), 200


app.run(port=8000)

from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.errors import ConnectionFailure, OperationFailure
from flask import Flask, jsonify, request

# Replace with your Atlas connection string
ATLAS_CONNECTION_STRING = "mongodb+srv://user:passWord@cluster.z4qr5p9.mongodb.net/?appName=Cluster"

client = MongoClient(ATLAS_CONNECTION_STRING)
db = client["ev_db"]         
app = Flask(__name__)

@app.route("/insert-fast", methods=["POST"])
def insert_fast():
    payload = request.json
    collections = db["vehicles"].with_options(write_concern=WriteConcern(w=1))
    id = collections.insert_one(payload)
    return jsonify(str(id.inserted_id))

@app.route("/insert-safe", methods=["POST"])
def insert_safe():
    payload = request.json
    collections = db["vehicles"].with_options(write_concern=WriteConcern(w="majority"))
    id = collections.insert_one(payload)
    return jsonify(str(id.inserted_id))

@app.route("/count-tesla-primary",methods=["GET"])
def count_tesla_primary():
    collections = db["vehicles"].with_options(read_preference=ReadPreference.PRIMARY)
    return jsonify({"count":collections.count_documents({"Make":"TESLA"})})

@app.route("/count-bmw-secondary",methods=["GET"])
def count_bmw_secondary():
    collections = db["vehicles"].with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    return jsonify({"count":collections.count_documents({"Make":"BMW"})})

if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True)

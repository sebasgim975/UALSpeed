from fastapi import FastAPI
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pydantic import BaseModel
import redis
import json
import time
import random
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Driver(BaseModel):
    name: str
    team: str
    nationality: str
    number: int


client = None
cloud_mode = False
local_metrics = {
    "home": 0,
    "drivers_get": 0,
    "drivers_post": 0,
    "drivers_delete": 0
}

try:

    client = MongoClient(
        "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
        serverSelectionTimeoutMS=5000
    )

    client.admin.command("ping")

    print("Connected to MongoDB Replica Set")

except Exception:

    print("MongoDB unavailable - running in cloud mode")
    cloud_mode = True


redis_client = None

try:

    redis_client = redis.Redis(
        host="redis",
        port=6379,
        decode_responses=True
    )

    redis_client.ping()

    print("Connected to Redis")

except Exception:

    print("Redis unavailable - running without cache")


# inicializar métricas globais Redis
if redis_client and not cloud_mode:
    redis_client.setnx("home", 0)
    redis_client.setnx("drivers_get", 0)
    redis_client.setnx("drivers_post", 0)
    redis_client.setnx("drivers_delete", 0)

    redis_client.setnx("drivers_get_total_time", 0)
    redis_client.setnx("drivers_get_count", 0)


if not cloud_mode:

    db = client["ualspeed"]
    drivers_collection = db["drivers"]

    drivers_collection.create_index("number", unique=True)


if not cloud_mode:

    while True:
        try:

            drivers_collection.update_one(
                {"number": 1},
                {
                    "$setOnInsert": {
                        "name": "Max Verstappen",
                        "team": "Red Bull",
                        "nationality": "Dutch",
                        "number": 1
                    }
                },
                upsert=True
            )

            drivers_collection.update_one(
                {"number": 44},
                {
                    "$setOnInsert": {
                        "name": "Lewis Hamilton",
                        "team": "Ferrari",
                        "nationality": "British",
                        "number": 44
                    }
                },
                upsert=True
            )

            print("Initial drivers checked/inserted")
            break

        except Exception:

            print("MongoDB primary not ready, retrying initialization...")
            time.sleep(5)

@app.get("/")
def home():
    if redis_client and not cloud_mode:        redis_client.incr("home")
    else:
        local_metrics["home"] += 1

    return {
        "message": "UALSpeed API running"
    }


@app.get("/drivers")
def get_drivers():
    if cloud_mode:

        return [
            {
                "name": "Cloud Driver",
                "team": "Render",
                "nationality": "Cloud",
                "number": 99
            }
        ]

    if redis_client and not cloud_mode:
        redis_client.incr("drivers_get")
    else:
        local_metrics["drivers_get"] += 1

    start_time = time.time()

    # verificar cache Redis
    cached_drivers = None

    if redis_client and not cloud_mode:
        cached_drivers = redis_client.get("drivers")
    if cached_drivers:

        print("Returning drivers from Redis cache")

        response_time = time.time() - start_time

        redis_client.incrbyfloat(
            "drivers_get_total_time",
            response_time
        )

        redis_client.incr("drivers_get_count")

        return json.loads(cached_drivers)

    print("Returning drivers from MongoDB")

    drivers = []

    for driver in drivers_collection.find({}, {"_id": 0}):
        drivers.append(driver)

    if redis_client and not cloud_mode:
        redis_client.set("drivers", json.dumps(drivers))

    response_time = time.time() - start_time

    redis_client.incrbyfloat(
        "drivers_get_total_time",
        response_time
    )

    redis_client.incr("drivers_get_count")

    return drivers


@app.post("/drivers")
def add_driver(driver: Driver):
    if redis_client and not cloud_mode:
        redis_client.incr("drivers_post")
    else:
        local_metrics["drivers_post"] += 1

    try:

        drivers_collection.insert_one(driver.dict())

        # adicionar à queue Redis
        redis_client.lpush(
            "drivers_queue",
            f"{driver.name} added"
        )

        # limpar cache Redis
        redis_client.delete("drivers")

        return {
            "message": "Driver added successfully"
        }

    except DuplicateKeyError:

        return {
            "message": "Driver number already exists"
        }


@app.delete("/drivers/{number}")
def delete_driver(number: int):
    if redis_client and not cloud_mode:
        redis_client.incr("drivers_delete")
    else:
        local_metrics["drivers_delete"] += 1

    result = drivers_collection.delete_one(
        {"number": number}
    )

    if result.deleted_count == 1:

        # limpar cache Redis
        redis_client.delete("drivers")

        return {
            "message": "Driver deleted"
        }

    else:

        return {
            "message": "Driver not found"
        }


@app.get("/metrics")
def metrics():
    if cloud_mode or not redis_client:

        total_requests = sum(local_metrics.values())

        return {
            "total_requests": total_requests,
            "requests_per_endpoint": local_metrics,
            "average_drivers_get_response_time_ms": 0
        }

    home = int(redis_client.get("home"))
    drivers_get = int(redis_client.get("drivers_get"))
    drivers_post = int(redis_client.get("drivers_post"))
    drivers_delete = int(redis_client.get("drivers_delete"))

    total_requests = (
        home
        + drivers_get
        + drivers_post
        + drivers_delete
    )

    total_time = float(
        redis_client.get("drivers_get_total_time")
    )

    get_count = int(
        redis_client.get("drivers_get_count")
    )

    average_response_time = 0

    if get_count > 0:

        average_response_time = (
            total_time / get_count
        ) * 1000

    return {

        "total_requests": total_requests,

        "requests_per_endpoint": {
            "home": home,
            "drivers_get": drivers_get,
            "drivers_post": drivers_post,
            "drivers_delete": drivers_delete
        },

        "average_drivers_get_response_time_ms":
            round(average_response_time, 2)
    }


@app.post("/metrics/reset")
def reset_metrics():

    redis_client.set("home", 0)
    redis_client.set("drivers_get", 0)
    redis_client.set("drivers_post", 0)
    redis_client.set("drivers_delete", 0)

    redis_client.set("drivers_get_total_time", 0)
    redis_client.set("drivers_get_count", 0)

    return {
        "message": "Metrics reset successfully"
    }


@app.get("/queue")
def get_queue():
    if cloud_mode or not redis_client:

        return {
            "queue": []
        }

    queue = redis_client.lrange(
        "drivers_queue",
        0,
        -1
    )

    return {
        "queue": queue
    }

@app.get("/telemetry")
def telemetry():

    return {
        "speed": random.randint(250, 350),
        "rpm": random.randint(9000, 15000),
        "track_position": random.randint(1, 20)
    }
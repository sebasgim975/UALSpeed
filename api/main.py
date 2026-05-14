from fastapi import FastAPI
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pydantic import BaseModel
import redis
import json
import time
import random
from fastapi.middleware.cors import CORSMiddleware

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

# esperar MongoDB Replica Set
while client is None:
    try:

        client = MongoClient(
            "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
            serverSelectionTimeoutMS=5000
        )

        client.admin.command("ping")

        print("Connected to MongoDB Replica Set")

    except Exception:

        print("MongoDB not ready, retrying...")
        time.sleep(5)


# ligação Redis
redis_client = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

print("Connected to Redis")


# inicializar métricas globais Redis
redis_client.setnx("home", 0)
redis_client.setnx("drivers_get", 0)
redis_client.setnx("drivers_post", 0)
redis_client.setnx("drivers_delete", 0)

redis_client.setnx("drivers_get_total_time", 0)
redis_client.setnx("drivers_get_count", 0)


# MongoDB
db = client["ualspeed"]
drivers_collection = db["drivers"]

# índice único
drivers_collection.create_index("number", unique=True)


# inserir dados iniciais sem duplicados
while True:
    try:

        drivers_collection.update_one(
            {"number": 1},
            {
                "$setOnInsert": {
                    "name": "Max Verstappen",
                    "team": "Red Bull",
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
                    "number": 44
                }
            },
            upsert=True
        )

        print("Initial drivers checked/inserted")

        break

    except Exception:

        print("MongoDB primary not ready, retrying...")
        time.sleep(5)


@app.get("/")
def home():

    redis_client.incr("home")

    return {
        "message": "UALSpeed API running"
    }


@app.get("/drivers")
def get_drivers():

    redis_client.incr("drivers_get")

    start_time = time.time()

    # verificar cache Redis
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

    # guardar cache Redis
    redis_client.set(
        "drivers",
        json.dumps(drivers)
    )

    response_time = time.time() - start_time

    redis_client.incrbyfloat(
        "drivers_get_total_time",
        response_time
    )

    redis_client.incr("drivers_get_count")

    return drivers


@app.post("/drivers")
def add_driver(driver: Driver):

    redis_client.incr("drivers_post")

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

    redis_client.incr("drivers_delete")

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
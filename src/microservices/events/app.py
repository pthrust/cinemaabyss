import json
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("events-service")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" 
TOPICS = {
    "movie": "movie-events",
    "user": "user-events",
    "payment": "payment-events"
}

producer: AIOKafkaProducer = None
consumer: AIOKafkaConsumer = None

async def start_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    logger.info("Kafka Producer started")

async def stop_kafka_producer():
    if producer:
        await producer.stop()
        logger.info("Kafka Producer stopped")

async def start_kafka_consumer():
    global consumer
    consumer = AIOKafkaConsumer(
        *TOPICS.values(),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="events-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    await consumer.start()
    logger.info("Kafka Consumer started")
    asyncio.create_task(consume_messages())

async def stop_kafka_consumer():
    if consumer:
        await consumer.stop()
        logger.info("Kafka Consumer stopped")

async def consume_messages():
    try:
        async for msg in consumer:
            logger.info(
                f"Received event: topic={msg.topic}, "
                f"partition={msg.partition}, offset={msg.offset}, "
                f"value={msg.value}"
            )
    except asyncio.CancelledError:
        logger.info("Consumer task cancelled")
    except Exception as e:
        logger.error(f"Consumer error: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):

    await start_kafka_producer()
    await start_kafka_consumer()
    yield

    await stop_kafka_consumer()
    await stop_kafka_producer()

app = FastAPI(title="Event Service", lifespan=lifespan)

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({
        "status": "ok",
        "service": "event service",
    })

@app.get("/api/events/health")
async def api_health():
    return {"status": True}

async def process_event(request: Request, topic: str):
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Invalid JSON"}
        )

    try:
        await producer.send(topic, payload)
        logger.info(f"Sent event to topic {topic}: {payload}")
    except Exception as e:
        logger.error(f"Failed to send event to {topic}: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Kafka error: {str(e)}"}
        )

    return JSONResponse(
        status_code=201,
        content={"status": "success", "topic": topic}
    )

@app.post("/api/events/movie")
async def create_movie_event(request: Request):
    return await process_event(request, TOPICS["movie"])

@app.post("/api/events/user")
async def create_user_event(request: Request):
    return await process_event(request, TOPICS["user"])

@app.post("/api/events/payment")
async def create_payment_event(request: Request):
    return await process_event(request, TOPICS["payment"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

# STEDI Human Balance Analytics – Real-Time Risk Scoring Pipeline
The STEDI Human Balance Analytics project simulates a real-time health monitoring platform designed to assess fall risk for seniors based on sensor data and repeated balance tests. The system collects customer assessment events, calculates individual risk scores, and visualizes population-wide risk trends through a live updating graph.
To enable this capability, the engineering and data streaming pipeline integrates Redis, Kafka, Kafka Connect, Spark Structured Streaming, and a STEDI web application.
This project focuses on building the complete streaming workflow needed to deliver risk scores by birth year to the STEDI dashboard.

## Project Goal
The primary objective is to create a streaming data pipeline that:

1. Consumes customer profile changes from Redis via Kafka (redis-server topic)
2. Consumes risk assessment events from the STEDI app via Kafka (stedi-events topic)
3. Decodes and parses customer data (base64 + JSON)
4. Parses fall-risk results (JSON)
5. Joins customer info with risk scores using email as the key
6. Produces enriched records containing customer email, birth year, risk score
7. Publishes results to a new Kafka topic (customer-risk)
8. Feeds the real-time UI graph in the STEDI application

The final output enables STEDI to visualize how fall-risk varies across customers’ birth years.

## Architecture Summary
Below is the high-level architecture implemented in this project:

![systems-diagram.png](https://github.com/shilpamadini/stedi-human-balance-redis-kafka-spark-streaming/blob/2f188e62d779135ed6ed0394fe783fc421adb372/systems-diagram.png)

Spark performs: Base64 decoding, JSON extraction, Windowless structured streaming, Real-time joins, Data enrichment, Writing the final stream back to Kafka (customer-risk)

The STEDI UI subscribes to the customer-risk topic and displays a dynamic graph of Risk Score by Birth Year.

### Key Components
Redis: Stores customer records including name, email, phone, and birthday.
Every update triggers a Redis Source Connector event to Kafka.

Kafka: Acts as the messaging backbone:

redis-server → raw customer updates
stedi-events → fall risk score events
customer-risk → enriched output for the STEDI UI

Kafka Connect: Streams Redis sorted-set entries into Kafka.

Spark Structured Streaming: Processes all real-time data

Reads from redis-server & stedi-events

Parses/decodes/cleans data

Performs streaming joins

Writes enriched customer risk results back to Kafka

STEDI Application
Consumes enriched Kafka topic and renders a live population risk graph.

Implementation
This project includes:

sparkpyrediskafkastreamtoconsole.py
Parses Redis events → prints customer email & birth year

sparkpyeventskafkastreamtoconsole.py
Parses fall risk events → prints customer & score

sparkpykafkajoin.py
Joins the two streams → writes enriched JSON to Kafka

Updated application.conf pointing to the new output topic

By the end of the pipeline execution, the STEDI application successfully receives real-time enriched data in the format:
{
  "customer": "Jason.Mitra@test.com",
  "score": 7.0,
  "email": "Jason.Mitra@test.com",
  "birthYear": "1975"
}

And displays a dynamic population risk graph that updates continuously as new data streams into the system.


## Setup using Docker 

You will need to use Docker to run the project on your own computer. You can find Docker for your operating system here: https://docs.docker.com/get-docker/

It is recommended that you configure Docker to allow it to use up to 2 cores and 6 GB of your host memory for use by the course workspace. If you are running other processes using Docker simultaneously with the workspace, you should take that into account also.



The docker-compose file at the root of the repository creates 9 separate containers:

- Redis
- Zookeeper (for Kafka)
- Kafka
- Banking Simulation
- Trucking Simulation
- STEDI (Application used in Final Project)
- Kafka Connect with Redis Source Connector
- Spark Master
- Spark Worker

It also mounts your repository folder to the Spark Master and Spark Worker containers as a volume  `/home/workspace`, making your code changes instantly available within to the containers running Spark.

Let's get these containers started!

```
cd [repositoryfolder]
docker-compose up
```

You should see 9 containers when you run this command:
```
docker ps
```

Docker Containers Used in This Project

The included docker-compose.yml file launches 9 different containers, each representing a component in the streaming architecture:

Container	Purpose
Redis	Stores customer records & events
Zookeeper	Metadata manager for Kafka
Kafka Broker	Message streaming engine
Banking Simulation	Source event generator for exercises
Trucking Simulation	Another event source
STEDI Application	Final project UI for risk graph
Kafka Connect (Redis Source Connector)	Streams Redis updates into Kafka
Spark Master	Coordinates Spark Structured Streaming
Spark Worker	Executes Spark streaming jobs

The compose file also mounts the local project folder into both Spark containers at:

/home/workspace


This allows your .py files to be edited on your computer while Spark picks them up instantly inside the container.

## Starting the Entire Environment

Open a terminal or PowerShell and navigate to your project root:

```
cd [repositoryfolder]
docker-compose up
```

Docker will start all 9 containers in the correct order.
You will see logs for:

Redis

Zookeeper

Kafka

Kafka Connect

Spark Master

Spark Worker

STEDI Application

Banking & Trucking simulations

Once everything is up, leave this terminal running.

Verifying Your Containers Are Running

In a second terminal:

```
docker ps

```
You should see exactly 9 running containers, including:

kafka

zookeeper

redis

spark-master

spark-worker

kafka-connect

stedi-application

simulation containers

Running Spark Streaming Scripts

Inside the Spark Master container, your scripts are located at:

/home/workspace


## Running Spark Streaming Scripts

Inside the Spark Master container, your scripts are located at:

```
/home/workspace
```

Execute the three streaming jobs like this:

1. Redis → Spark → Console
```
spark-submit sparkpyrediskafkastreamtoconsole.py
```

2. stedi-events → Spark → Console

```
spark-submit sparkpyeventskafkastreamtoconsole.py
```

3. Redis + Events Join → Kafka (customer-risk topic)

```
spark-submit sparkpykafkajoin.py
```

## Viewing Output in Kafka Topics

Inspect Kafka messages using:

```
kafka-console-consumer --bootstrap-server kafka:9092 --topic redis-server --from-beginning
kafka-console-consumer --bootstrap-server kafka:9092 --topic stedi-events --from-beginning
kafka-console-consumer --bootstrap-server kafka:9092 --topic customer-risk --from-beginning
```

customer-risk is the topic consumed by the STEDI application UI.

## Opening the STEDI Application

Once the join job is running and producing messages, open the UI in your browser:

http://localhost:3000


Navigate to "Risk Scores by Birth Year"
As events stream in, you should see the graph update in real time.


## Summary

This setup allows you to run the entire pipeline locally:

Redis → Kafka via Redis Source Connector
Kafka → Spark Structured Streaming
Spark Join → Kafka (customer-risk)
STEDI Web App consumes the joined stream
Live graph updates in UI

Everything runs in isolated Docker containers, ensuring consistent behavior across machines.






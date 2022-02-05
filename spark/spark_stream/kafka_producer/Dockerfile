FROM python:3.7-slim

WORKDIR /usr/src/app_asvsp

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="localhost:9092"




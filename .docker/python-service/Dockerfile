FROM python:3.10-slim-buster
RUN apt-get update && apt-get install bash python3-dev gdal-bin build-essential libssl-dev libffi-dev -y

WORKDIR /service
COPY . /service
RUN pip install -r requirements.txt

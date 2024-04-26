FROM python:3.11-slim-bookworm

WORKDIR /usr/app/arboviroses/

COPY requirements.txt /usr/app/arboviroses/
# COPY dbt/ /usr/app/arboviroses/dbt/

RUN apt update
RUN pip install -r requirements.txt
WORKDIR /usr/app/arboviroses/

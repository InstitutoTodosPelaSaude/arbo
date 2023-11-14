FROM python:3.11-slim-bookworm

WORKDIR /usr/app/arboviroses/

COPY requirements.txt /usr/app/arboviroses/

RUN apt update
RUN pip install -r requirements.txt

RUN dbt seed

CMD ["dagster", "dev"]
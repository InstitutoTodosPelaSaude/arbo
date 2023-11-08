FROM ghcr.io/dbt-labs/dbt-core:1.6.5

RUN pip install --upgrade pip
RUN pip install --upgrade dbt-postgres

COPY profiles.yml /root/.dbt/profiles.yml

WORKDIR /usr/app/arboviroses/

# Keep container running
ENTRYPOINT [ "tail", "-f", "/dev/null" ]
#!/bin/bash

# Run dbt deps
dbt deps --project-dir /usr/app/arboviroses/dbt/

# Start dagster-webserver
dagster-webserver -h 0.0.0.0 -p 3000
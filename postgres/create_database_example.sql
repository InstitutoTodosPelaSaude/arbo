\c itps;
CREATE SCHEMA arboviroses;
GRANT ALL PRIVILEGES ON DATABASE itps TO itps;

CREATE DATABASE itps_dev;
\c itps_dev;

CREATE USER itps_dev WITH PASSWORD 'itps_dev_pass';
GRANT ALL PRIVILEGES ON DATABASE itps_dev TO itps_dev;

CREATE SCHEMA arboviroses;
GRANT ALL PRIVILEGES ON SCHEMA arboviroses TO itps_dev;
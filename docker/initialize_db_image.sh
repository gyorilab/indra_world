#!/bin/bash
# This script initializes the INDRA World postgres database as a local
# docker image called postgres:indra_world. The postgres DB in the
# image will have the necessary schema in it but will otherwise be blank.

# This is the password that will be used to access the DB. It is recommended
# that this be changed to some other secret value.
POSTGRES_PASSWORD="mysecretpassword"
# This is a port that needs to be temporarily exposed to initialize the DB.
# Does not need to be changed unless there is something else running on that
# port.
TEMP_CONTAINER_PORT=5434

# We first pull the generic postgres image
docker pull postgres:latest
# We next run the postgres container and get its container ID
container_id=$(docker run --rm -p $TEMP_CONTAINER_PORT:5432 -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -d -e PGDATA=/var/lib/postgresql/pgdata postgres:latest)
# We can now initialize the DB using the INDRA World schema. This requires
# having indra_world on the importable Python path (e.g., PYTHONPATH).
python -c "from indra_world.service.db import DbManager;
db = DbManager('postgresql://postgres:$POSTGRES_PASSWORD@localhost:$TEMP_CONTAINER_PORT');
db.create_all()"
# We now commit the Docker image with the schema baked in
docker commit $container_id indra_world_db:latest
# We can finally stop the container
docker stop $container_id

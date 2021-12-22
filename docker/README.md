Dockerized INDRA World service
==============================
This folder contains files to build necessary Docker images and run them.
The INDRA World service requires a postgres database and we assume here
that that database is available as a separate local image called postgres:indra_world,
though any other local or remote DB can be used as well.

Building the INDRA World service image
--------------------------------------
To build the Docker image, run

```
docker build --tag indra_world:latest .
```

Initializing the INDRA World DB image
-------------------------------------
To create the `postgres:indra_world` Docker image from scratch, run

```
./initialize_db_image.sh
```

Running the integrated service
------------------------------
A docker-compose file defines how the service image and DB image need to be
run. To launch the service, run

```
docker-compose up -d
```
where the optional `-d` flag runs the containers in the background.

There are two files that need to be defined containing environment
variables for each container with the following names and content:

`indra_world.env`
```
INDRA_WM_SERVICE_DB=postgres://postgres:<password for local postgres>@db:5432
DART_WM_USERNAME=<DART username>
DART_WM_PASSWORD=<DART password>
AWS_ACCESS_KEY_ID=<AWS account key ID, necessary if assembled outputs need to be dumped to S3 for CauseMos>
AWS_SECRET_ACCESS_KEY=<AWS account secret key, necessary if assembled outputs need to be dumped to S3 for CauseMos>
AWS_REGION=us-east-1
INDRA_WORLD_ONTOLOGY_URL=<GitHub URL to ontology being used>
```

`indra_world_db.env`
```
POSTGRES_PASSWORD=<password for local postgres>
PGDATA=/var/lib/postgresql/pgdata
```


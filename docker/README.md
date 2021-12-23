Dockerized INDRA World service
==============================
This folder contains files to run the INDRA World service through Docker
containers. It also provides files to build them locally in case
customizations are needed.

Running the integrated service
------------------------------
A docker-compose file defines how the service image and DB image need to be
run. The docker-compose file refers to two images (`indralab/indra_world`
and `indralab/indra_world_db`), both available publicly
on Dockerhub. This means that they are automatically pulled when running
`docker-compose up` unless they are already available locally.

To launch the service, run

```
docker-compose up -d
```
where the optional `-d` flag runs the containers in the background.

There are two files that need to be created containing environment
variables for each container with the following names and content:

`indra_world.env`
```
INDRA_WM_SERVICE_DB=postgresql://postgres:<password for local postgres>@db:5432
DART_WM_URL=<DART URL>
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

Building the Docker images locally
-----------------------------------

As described above, the two necessary Docker images are available on Dockerhub,
therefore the following steps are only necessary if local changes to the
images (beyond what can be controlled through environmental variables)
are needed.

Building the INDRA World service image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To build the `indra_world` Docker image, run

```
docker build --tag indra_world:latest .
```

Initializing the INDRA World DB image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To create the `indra_world_db` Docker image from scratch, run

```
./initialize_db_image.sh
```

Note that this requires Python dependencies needed to run
INDRA World to be available in the local environment.

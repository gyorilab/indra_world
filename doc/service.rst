World Modelers INDRA service stack
==================================

.. _wm-service-endpoints:

Using the INDRA World API
-------------------------
The API is deployed and documented at `wm.indra.bio <http://wm.indra.bio/>`_.

.. _wm-service-local-setup:

Setting up the INDRA World API locally
--------------------------------------
These instructions describe setting up and using the INDRA service stack
for World Modelers applications.

First, you need to build the INDRA World Docker image as follows:

.. code-block:: sh

    git clone https://github.com/indralab/indra_world.git
    cd indra_world/docker
    docker build --tag indra_world:latest .

Then, in the same folder, do:

.. code-block:: sh

    docker-compose up -d

to run the INDRA world service as well as an associated postgres container
with the relational database used by the service. The `docker-compose` file
reads secret configuration values for accessing various resources from two
files: `indra_world.env` and `indra_world_db.env`. These files are not part
of the public code and need to be added manually.

.. _wm-service-s3:

INDRA assemblies on S3
----------------------
Access to the INDRA-assembled corpora requires credentials to the shared
World Modelers S3 bucket "world-modelers". Each INDRA-assembled corpus is
available within this bucket, under the "indra_models" key base. Each corpus
is identified by a string identifier.

The corpus index
~~~~~~~~~~~~~~~~
The list of corpora can be obtained either using S3's list objects function
or by reading the index.csv file which is maintained by INDRA. This index
is a comma separated values text file which contains one row for each corpus.
Each row's first element is a corpus identifier, and the second element
is the UTC date-time at which the corpus was uploaded to S3. An example
row in this file looks as follows

.. code-block:: sh

    test1_newlines,2020-05-08-22-34-29

where test1_newlines is the corpus identifier and 2020-05-08-22-34-29 is the
upload date-time.

Structure of each corpus
~~~~~~~~~~~~~~~~~~~~~~~~
Within the world-modelers bucket, under the indra_models key base, files
for each corpus are organized under a subkey equivalent to the corpus
identifier, for instance, all the files for the test1_newlines corpus
are under the indra_models/test1_newlines/ key base. The list of files
for each corpus are as follows

- `statements.json`: a JSON dump of assembled INDRA Statements. As of May 2020,
  each statement's JSON representation is on a separate line in this file.
  Any corpus uploaded before that has a standard JSON structure. This is the
  main file that CauseMos needs to ingest for UI interaction.

- `raw_statements.json`: a JSON dump of raw INDRA Statements. This file is
  typically not needed in downstream usage, however, the INDRA curation
  service needs to have access to it for internal assembly tasks.

- `metadata.json`: a JSON file containing key-value pairs that describe the
  corpus. The standard keys in this file are as follows:

  - `corpus_id`: the ID of the corpus (redundant with the corresponding entry
    in the index).
  - `description`: a human-readable description of how the corpus was obtained.
  - `display_name`: a human-readable display name for the corpus.
  - `readers`: a list of the names of the reading systems from which
    statements were obtained in the corpus.
  - `assembly`: a dictionary identifying attributes of the assembly process with
    the following keys:

      - `level`: the level of resolution used to assemble the corpus
        (e.g., "location_and_time").
      - `grounding_threshold`: the threshold (if any) which was used to filter
        statements by grounding score (e.g., 0.7)

  - `num_statements`: the number of assembled INDRA Statements in the corpus (
    i.e., statements.json).
  - `num_documents`: the number of documents that were read by readers to
    produce the statements that were assembled.

  Note that any of these keys may be missing if unavailable, for instance,
  in the case of old uploads.

- `curations.json`: a JSON file which persists curations as collected by INDRA.
  This is the basis of surfacing reader-specific curations in the
  download_curation endpoint (see above).


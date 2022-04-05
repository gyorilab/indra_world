.. mdinclude:: ../docker/README.md

.. _wm-service-endpoints:

Using the public INDRA World API
--------------------------------
The API is deployed and documented at `wm.indra.bio <http://wm.indra.bio/>`_.

.. _wm-service-s3:

Cloud-based CauseMos integration via S3
---------------------------------------
Access to the INDRA-assembled corpora requires credentials to the shared
World Modelers S3 bucket "world-modelers". Each INDRA-assembled corpus is
available within this bucket, under the "indra_models" key base. Each corpus
is identified by a string identifier.

The corpus index
^^^^^^^^^^^^^^^^
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

.. _wm-service-corpus-structure:

Structure of each corpus
^^^^^^^^^^^^^^^^^^^^^^^^
Within the world-modelers bucket, under the indra_models key base, files
for each corpus are organized under a subkey equivalent to the corpus
identifier, for instance, all the files for the test1_newlines corpus
are under the indra_models/test1_newlines/ key base. The list of files
for each corpus are as follows

- `statements.json`: a JSON dump of assembled INDRA Statements.
  Each statement's JSON representation is on a separate line in this file.
  This is the main file that CauseMos needs to ingest for UI interaction.

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
  - `tenant`: if DART is used, a corpus is typically associated with a tenant
    (i.e., a user or an institution); this field provides the tenant ID.

Note that any of these keys may be missing if unavailable, for instance,
in the case of old uploads.


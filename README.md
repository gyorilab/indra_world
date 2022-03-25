# INDRA World

<img align="left" src="https://raw.githubusercontent.com/indralab/indra_world/master/doc/indra_world_logo.png" width="300" height="160" />
INDRA World is a generalization of INDRA (originally developed for biology) for
automatically collecting, assembling, and modeling the web of causal relations
that drive interconnected events in regional and global systems.

INDRA World interfaces with four machine reading systems which extract
concepts, events, and causal relations from text (typically reports from
governmental and non-governmental organizations, news stories, and scientific
publications). The extractions are processed into a standardized Statement
representation and then processed (filtered, normalized, etc.).

INDRA World makes use of the general INDRA assembly logic to find relationships
between statements, including matching, contradiction, and refinement (i.e.,
one statement is a more general or more specific version of the other).  It
then calculates a belief score which is based on all available evidence
directly or indirectly supporting a given statement.

This repository also implements a database and service architecture to run
INDRA World as a service that integrates with other systems and supports
managing project-specific statement sets and incremental assembly with new
reader outputs.

## Installation

INDRA World can be installed directly from Github as

    $ pip install git+https://github.com/indralab/indra_world.git

Additionally, INDRA World can be run as a dockerized service.
For more information, see https://github.com/indralab/indra_world/tree/master/docker.

## Documentation

Detailed documentation is available at:
https://indra-world.readthedocs.io/en/latest/.

## Command line interface

The INDRA World command line interface allows running assembly using externally
supplied arguments and configurations files. This serves as an alternative
to using the Python API.

```
usage: indra_world [-h]
                   (--reader-output-files READER_OUTPUT_FILES |
                    --reader-output-dart-query READER_OUTPUT_DART_QUERY |
                    --reader-output-dart-keys READER_OUTPUT_DART_KEYS)
                   [--assembly-config ASSEMBLY_CONFIG]
                   (--ontology-path ONTOLOGY_PATH |
                    --ontology-id ONTOLOGY_ID)
                    --output-folder OUTPUT_FOLDER
                   [--causemos-metadata CAUSEMOS_METADATA]

INDRA World assembly CLI

optional arguments:
  -h, --help            show this help message and exit

Input options:
  --reader-output-files READER_OUTPUT_FILES
                        Path to a JSON file whose keys are reading system
                        identifiers and whose values are lists of file paths to
                        outputs from the given system to be used in assembly.
  --reader-output-dart-query READER_OUTPUT_DART_QUERY
                        Path to a JSON file that specifies query parameters for
                        reader output records in DART.  Only applicable if DART
                        is being used.
  --reader-output-dart-keys READER_OUTPUT_DART_KEYS
                        Path to a text file where each line is a DART storage
                        key corresponding to a reader output record. Only
                        applicable if DART is being used.

Assembly options:
  --assembly-config ASSEMBLY_CONFIG
                        Path to a JSON file that specifies the INDRA assembly
                        pipeline. If not provided, the default assembly
                        pipeline will be used.
  --ontology-path ONTOLOGY_PATH
                        Path to an ontology YAML file.
  --ontology-id ONTOLOGY_ID
                        The identifier of an ontology registered in DART. Only
                        applicable if DART is being used.

Output options:
  --output-folder OUTPUT_FOLDER
                        The path to a folder to which the INDRA output will be
                        written.
  --causemos-metadata CAUSEMOS_METADATA
                        Path to a JSON file that provides metadata to be used
                        for a Causemos-compatible dump of INDRA output (which
                        consists of multiple files). THe --output-path
                        option must also be used along with this option.
```

The CLI can also be invoked through Docker. In this case, all CLI arguments
that are paths, need to be made visible to Docker. To do this, the -v flag can
be used to mount a host folder (in the command below, [local-path-to-mount]
into the container on a given path.  All CLI path arguments then need to be
given with respect to the path as seen in the container. Furthermore, if any of
the files referred to in CLI arguments themselves list file paths (e.g., the
value of --reader-output-files), those paths need to be relative to the Docker
container's mounted volume as well.

```
docker run -v [local-path-to-mount]:/data --entrypoint indra_world indralab/indra_world:latest [cli-arguments]
```

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

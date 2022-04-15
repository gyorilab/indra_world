FROM labsyspharm/indra:latest

RUN git clone https://github.com/indralab/indra_world.git && \
    cd indra_world && \
    pip install -e . && \
    cd ../indra && \
    git pull && \
    pip install -U -e . && \
    mkdir /sw/dart_cache
    mkdir /data

ENTRYPOINT ["gunicorn", "-w", "1", "-b", ":8000", "-t", "600", "indra_world.service.app:app"]

import os

HERE = os.path.dirname(os.path.abspath(__file__))


def get_resource_file(fname):
    return os.path.join(HERE, fname)
"""A client for accessing reader output from the DART system."""
__all__ = ['DartClient', 'prioritize_records']
import os
import tqdm
import json
import glob
import logging
import requests
import itertools
from typing import Optional
from datetime import datetime
from collections import defaultdict
from indra.config import get_config
from .api import get_unique_records


logger = logging.getLogger(__name__)

default_dart_url = ('https://wm-ingest-pipeline-rest-1.prod.dart.'
                    'worldmodelers.com/dart/api/v1')


class DartClient:
    """A client for the DART web service with optional local storage.

    Parameters
    ----------
    storage_mode : Optional[str]
        If `web`, the configured DART URL and credentials are used to
        communicate with the DART web service. If `local`, a local storage
        is used to access and store reader outputs.
    dart_url : Optional[str]
        The DART service URL. If given, it overrides the DART_WM_URL
        configuration value.
    dart_uname : Optional[str]
        The DART service user name. If given, it overrides the DART_WM_USERNAME
        configuration value.
    dart_pwd : Optional[str]
        The DART service password. If given, it overrides the DART_WM_PASSWORD
        configuration value.
    local_storage : Optional[str]
        A path that points to a folder for local storage. If the storage_mode
        is `web`, this local_storage is used as a local cache. If the
        storage_mode is `local`, it is used as the primary location to access
        reader outputs. If given, it overrides the INDRA_WM_CACHE configuration
        value.
    """
    def __init__(self, storage_mode='web', dart_url=None, dart_uname=None,
                 dart_pwd=None, local_storage=None):
        self.storage_mode = storage_mode
        # We set the local storage in either mode, since even in web mode
        # it is used as a cache
        self.local_storage = local_storage if local_storage else \
            get_config('INDRA_WM_CACHE')
        # In web mode, we try to get a URL, a username and a password. In order
        # of priority, we first take arguments provided directly, otherwise
        # we take configuration values.
        if self.storage_mode == 'web':
            if dart_url:
                self.dart_url = dart_url
            else:
                dart_config_url = get_config('DART_WM_URL')
                self.dart_url = dart_config_url if dart_config_url else \
                    default_dart_url
            # for backwards compatibility, make sure we are using the base
            # URL here.
            if self.dart_url and self.dart_url.endswith('/readers'):
                self.dart_url = self.dart_url[:-8]
            if dart_uname:
                self.dart_uname = dart_uname
            else:
                self.dart_uname = get_config('DART_WM_USERNAME')
            if dart_pwd:
                self.dart_pwd = dart_pwd
            else:
                self.dart_pwd = get_config('DART_WM_PASSWORD')
            if not self.dart_uname or not self.dart_pwd:
                logger.warning('DART is used in web mode but username or '
                               'password were not provided or set in the '
                               'DART_WM_USERNAME and DART_WM_PASSWORD '
                               'configurations.')
        # In local mode, we need to have a local storage set
        else:
            self.dart_url = None
            if not self.local_storage:
                raise ValueError('DART client initialized in local mode '
                                 'without a local storage path.')
        # If the local storage doesn't exist, we try create the folder
        if self.local_storage and (not os.path.exists(self.local_storage)):
            logger.info('The local storage path %s for the DART client '
                        'doesn\'t exist and will now be created' %
                        self.local_storage)
            try:
                os.makedirs(self.local_storage)
            except Exception as e:
                logger.error('Could not create DART client local storage: %s'
                             % e)
        logger.info('Running DART client in %s mode with local storage at %s' %
                    (self.storage_mode, self.local_storage))

    def get_outputs_from_records(self, records):
        """Return reader outputs corresponding to a list of records.

        Parameters
        ----------
        records : list of dict
            A list of records returned from the reader output query.

        Returns
        -------
        dict(str, dict)
            A two-level dict of reader output keyed by reader and then
            document id.
        """
        # Loop document keys and get documents
        reader_outputs = defaultdict(dict)
        for record in tqdm.tqdm(records):
            reader_outputs[record['identity']][record['document_id']] = \
                self.get_output_from_record(record)
        reader_outputs = dict(reader_outputs)
        return reader_outputs

    def get_output_from_record(self, record):
        """Return reader output corresponding to a single record.

        Parameters
        ----------
        record : dict
            A single DART record.

        Returns
        -------
        str
            The reader output corresponding to the given record.
        """
        storage_key = record['storage_key']
        fname = self.get_local_storage_path(record)
        output = None
        if fname and os.path.exists(fname):
            with open(fname, 'r') as fh:
                output = fh.read()
        elif self.storage_mode == 'web':
            try:
                output = self.download_output(storage_key)
            except Exception as e:
                logger.warning('Error downloading %s: %s' %
                               (storage_key, e))
                return None
            try:
                if self.local_storage:
                    with open(fname, 'w') as fh:
                        fh.write(output)
            except Exception as e:
                logger.warning('Error storing %s: %s' %
                               (storage_key, e))
        else:
            logger.info('Record with storage key %s doesn\'t exist '
                        'in local storage.' % storage_key)
        return output

    def cache_record(self, record, overwrite=False):
        """Download and cache a given record in local storage.

        Parameters
        ----------
        record : dict
            A DART record.
        """
        fname = self.get_local_storage_path(record)
        if overwrite or not os.path.exists(fname):
            output = self.download_output(record['storage_key'])
            with open(fname, 'w') as fh:
                fh.write(output)

    def cache_records(self, records, overwrite=False):
        """Download and cache a list of records in local storage.

        Parameters
        ----------
        records : list[dict]
            A list of DART records.
        """
        for record in tqdm.tqdm(records):
            self.cache_record(record, overwrite=overwrite)

    def download_output(self, storage_key):
        """Return content from the DART web service based on its storage key.

        Parameters
        ----------
        storage_key : str
            A DART storage key.

        Returns
        -------
        str
            The content corresponding to the storage key.
        """
        url = self.dart_url + '/readers/download/%s' % storage_key
        res = requests.get(url=url, auth=(self.dart_uname, self.dart_pwd))
        res.raise_for_status()
        return res.text

    def get_local_storage_path(self, record):
        """Return the local storage path for a DART record."""
        if not self.local_storage:
            return None
        folder = os.path.join(self.local_storage, record['identity'],
                              record['version'])
        if not os.path.exists(folder):
            os.makedirs(folder)
        fname = os.path.join(folder, record['document_id'])
        return fname

    def get_reader_output_records(self, readers=None, versions=None,
                                  document_ids=None, timestamp=None,
                                  tenant=None, ontology_id=None,
                                  unique=False):
        """Return reader output metadata records by querying the DART API

        Query json structure:
            {"readers": ["MyAwesomeTool", "SomeOtherAwesomeTool"],
            "versions": ["3.1.4", "1.3.3.7"],
            "document_ids": ["qwerty1234", "poiuyt0987"],
            "timestamp": {"before": "yyyy-mm-ddThh:mm:ss",
            "after": "yyyy-mm-ddThh:mm:ss"}}

        Parameters
        ----------
        readers : list
            A list of reader names
        versions : list
            A list of versions to match with the reader name(s)
        document_ids : list
            A list of document identifiers
        timestamp : dict("before"|"after",str)
            The timestamp string must be formatted "yyyy-mm-ddThh:mm:ss".
        tenant : Optional[str]
            Return only records for the given tenant.
        ontology_id : Optional[str]
            Return only records for the given ontology ID.
        unique : Optional[bool]
            If true, records that are duplicates are collapsed. Default: False.

        Returns
        -------
        dict
            The JSON payload of the response from the DART API
        """
        if self.storage_mode == 'web':
            query_data = _jsonify_query_data(readers, versions, document_ids,
                                             timestamp)
            if not query_data:
                return {}
            full_query_data = {'metadata': query_data}
            url = self.dart_url + '/readers/query'
            res = requests.post(url, data=full_query_data,
                                auth=(self.dart_uname, self.dart_pwd))
            res.raise_for_status()
            rj = res.json()

            # This handles both empty list and dict
            if not rj or 'records' not in rj:
                records = []
            else:
                records = rj['records']
        else:
            records = []
            if readers:
                if versions:
                    for reader, version in itertools.product(readers, versions):
                        path = os.path.join(self.local_storage, reader,
                                            version, '*')
                        for file in glob.glob(path):
                            doc_id = os.path.basename(file)
                            record = {
                                'identity': reader,
                                'version': version,
                                'doc_id': doc_id,
                                'storage_key': file
                            }
                            records.append(record)
                else:
                    for reader in readers:
                        path = os.path.join(self.local_storage, reader, '*')
                        version_paths = glob.glob(path)
                        for version_path in version_paths:
                            version = os.path.basename(version_path)
                            path = glob.glob(version_path, '*')
                            for file in glob.glob(path):
                                doc_id = os.path.basename(file)
                                record = {
                                    'identity': reader,
                                    'version': version,
                                    'doc_id': doc_id,
                                    'storage_key': file
                                }
                                records.append(record)
                if document_ids:
                    records = [r for r in records
                               if r['doc_id'] in document_ids]
            else:
                raise ValueError('Must provide readers for searching in local '
                                 'mode.')
        if ontology_id:
            records = [r for r in records if r['output_version'] == ontology_id]
        if tenant:
            records = [r for r in records if r['tenants'] and
                       tenant in r['tenants']]
        if unique:
            records = get_unique_records(records)
        return records

    def get_reader_versions(self, reader):
        """Return the available versions for a given reader."""
        records = self.get_reader_output_records([reader])
        return {record['version'] for record in records}

    def get_ontology(self, ontology_id: str):
        """Return the DART ontology record JSON for the given ontology ID."""
        url = self.dart_url + '/ontologies'
        res = requests.get(url, params={'id': ontology_id},
                           auth=(self.dart_uname, self.dart_pwd))
        return res.json()

    def get_tenant_ontology(self, tenant_id: str,
                            version: Optional[str] = None):
        """Return the DART ontology record JSON for the given tenant ID and
        optional version."""
        params = {'tenant': tenant_id}
        if version:
            params['version'] = version
        url = self.dart_url + '/ontologies'
        res = requests.get(url, params=params,
                           auth=(self.dart_uname, self.dart_pwd))
        return res.json()

    def get_ontology_graph(self, ontology_id: str):
        """Return the ontology graph for the given ontology ID."""
        ont_json = self.get_ontology(ontology_id)
        return self._get_ontology_graph_from_json(ont_json)

    def get_tenant_ontology_graph(self, tenant_id: str,
                                  version: Optional[str] = None):
        """Return the ontology graph for the given tenant ID and optional
        version."""
        ont_json = self.get_tenant_ontology(tenant_id, version=version)
        return self._get_ontology_graph_from_json(ont_json)

    @staticmethod
    def _get_ontology_graph_from_json(ont_json):
        import yaml
        from indra_world.ontology import WorldOntology
        ontology = \
            WorldOntology(url=None,
                          yml=yaml.load(ont_json['ontology'],
                                        Loader=yaml.FullLoader))
        ontology.initialize()
        return ontology



def prioritize_records(records, priorities=None):
    """Return unique records per reader and document prioritizing by version.

    Parameters
    ----------
    records : list of dict
        A list of records returned from the reader output query.
    priorities : dict of list
        A dict keyed by reader names (e.g., cwms, eidos) with values
        representing reader versions in decreasing order of priority.

    Returns
    -------
    records : list of dict
        A list of records that are unique per reader and document, picked by
        version priority when multiple records exist for the same reader
        and document.
    """
    priorities = {} if not priorities else priorities
    prioritized_records = []
    key = lambda x: (x['identity'], x['document_id'])
    for (reader, doc_id), group in itertools.groupby(sorted(records, key=key),
                                                     key=key):
        group_records = list(group)
        if len(group_records) == 1:
            prioritized_records.append(group_records[0])
        else:
            reader_prio = priorities.get(reader)
            if reader_prio:
                first_rec = sorted(
                    group_records,
                    key=lambda x: reader_prio.index(x['version']))[0]
                prioritized_records.append(first_rec)
            else:
                logger.warning('Could not prioritize between records: %s' %
                               str(group_records))
                prioritized_records.append(group_records[0])
    return prioritized_records


def _check_lists(lst):
    if not isinstance(lst, (list, tuple)):
        return False
    elif any(not isinstance(s, str) for s in lst):
        logger.warning('At least one object in list is not a string')
        return False
    return True


def _check_timestamp_dict(ts_dict):
    """Check the timestamp dict

    Parameters
    ----------
    ts_dict : dict
        Timestamp should be of format "yyyy-mm-ddThh:mm:ss".

    Returns
    -------
    dict
    """
    def _is_valid_ts(tstr):
        """
        %Y - Year as Zero padded decimal
        %m - month as zero padded number
        %d - day as zero padded number
        %H - 24h hour as zero padded number
        %M - minute as zero padded number
        %S - second as zero padded number
        """
        ts_long_fmt = '%Y-%m-%dT%H:%M:%S'
        try:
            dt = datetime.strptime(tstr, ts_long_fmt)
        except ValueError as err:
            raise ValueError(
                f'Timestamp "{tstr}" is not in a valid format. '
                f'Format must be "%Y-%m-%dT%H:%M:%S"') from err
        try:
            if dt < datetime(1900, 1, 1):
                logger.warning('Timestamp is before 1900-JAN-01, ignoring')
                return False
        except (ValueError, OverflowError):
            logger.warning('Could not parse timestamp, ignoring')
            return False
        return True

    ek = {'before', 'after'}
    if sum(k in ek for k in ts_dict) > 0:
        ts = {k: v for k, v in ts_dict.items() if k in ek and
              _is_valid_ts(v)}
    else:
        raise ValueError(f'None of the allowed keys '
                         f'{", ".join(list(ek))} were provided')
    return ts


def _jsonify_query_data(readers=None, versions=None, document_ids=None,
                        timestamp=None):
    """Check and json.dumps the metadata dictionary

    Parameters
    ----------
    readers : list
        The list of reading systems.
    versions : list
        Versions of reading systems.
    document_ids : list
        Document IDs.
    timestamp : dict("before"|"after",str)
        Reader output time stamp constraint.

    Returns
    -------
    str
        The json.dumps representation of the query metadata
    """
    if all(v is None for v in [readers, versions, document_ids, timestamp]):
        logger.warning('No query parameters were filled out')
        return ''
    pd = {}
    if readers and _check_lists(readers):
        pd['readers'] = readers
    if versions and _check_lists(versions):
        pd['versions'] = versions
    if document_ids and _check_lists(document_ids):
        pd['document_ids'] = document_ids
    if isinstance(timestamp, dict):
        pd['timestamp'] = _check_timestamp_dict(timestamp)
    elif timestamp is not None:
        raise ValueError('Argument "timestamp" must be of type dict')

    return json.dumps(pd)

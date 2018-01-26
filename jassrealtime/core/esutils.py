# coding: utf-8

import re, string, sys
import logging
from typing import List
from datetime import datetime
from elasticsearch_dsl import Search, Q
from elasticsearch.exceptions import TransportError
from urllib3.exceptions import NewConnectionError
import time

from elasticsearch import Elasticsearch
from .settings_utils import get_settings, get_scan_scroll_duration, get_nb_documents_per_scan_scroll

_ES_CONN = None
ES_DATE_FORMAT = "yyy-MM-dd HH:mm:ss"
ES_TO_DATETIME_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
SLEEP_TIME_BEFORE_RECONNECT = 3  # sleep time in seconts
NB_OF_RECONNECTS = 50
MAX_SMALL_SEARCH_NO_SCAN_RESULTS = 10000  # Search can return at most 10000 results. Default ES configration


class ESWaitTooLongForReadyState(Exception):
    pass


class ESInvalidIndexName(Exception):
    pass


class ESConnectionFailed(Exception):
    pass


def es_wait_ready():
    """
    This function will wait configured number of  seconds for the cluster to be ready. Will raise cluster

    :return:
    """
    timeout = get_settings()['ELASTIC_SEARCH']['cluster_health_timeout']
    es = get_es_conn()
    es.cluster.health(wait_for_status="yellow", timeout=str(timeout) + "s")


def get_es_conn():
    logger = logging.getLogger(__name__)
    global _ES_CONN
    sett = get_settings()
    if _ES_CONN is None:
        count = 0
        while count < NB_OF_RECONNECTS:
            try:
                _ES_CONN = Elasticsearch(
                    sett['ELASTIC_SEARCH']['hosts'],
                    # sniff before doing anything
                    sniff_on_start=sett['ELASTIC_SEARCH']['sniff_on_start'],
                    # refresh nodes after a node fails to respond
                    sniff_on_connection_fail=sett['ELASTIC_SEARCH']['sniff_on_connection_fail'],
                    # and also every 60 seconds
                    sniffer_timeout=sett['ELASTIC_SEARCH']['sniffer_timeout'],
                    # timeout used for the sniff request
                    sniff_timeout=sett['ELASTIC_SEARCH']['sniff_timeout'],
                    # maximum number of parallel connections
                    maxsize=sett['ELASTIC_SEARCH']['maxsize']
                )
                logger.info("Connected to ES located on:" + str(sett['ELASTIC_SEARCH']['hosts']))
                return _ES_CONN
            except:
                logger.error("Connections to jass failed. Attemp {0}/{1}".format(
                    count, NB_OF_RECONNECTS))
                time.sleep(SLEEP_TIME_BEFORE_RECONNECT)
                count = count + 1

        raise ESConnectionFailed()

    return _ES_CONN


def string_to_id(str: str):
    """
    Converts a random string to a valid id

    :param str: Input String
    :return: ID
    """
    return re.sub(r'[^\w_-]+', '', str).lower()


def is_nested_dict_equal(d1: dict, d2: dict, path=""):
    for k in d1.keys():
        if not k in d2:
            return False
        else:
            if type(d1[k]) is dict:
                is_nested_dict_equal(d1[k], d2[k], path)
            else:
                if d1[k] != d2[k]:
                    return False

    return True


def delete_indices(indices, envId: str, classSuffix: str):
    """
    Deletes a list of indices
    Throws ESInvalidIndexName if index name is invalid

    :param indices:         One index, multiple indices separated by coma. One index may represent multiple by using star at the end
    :param envId:          envId name
    :param classSuffix:     Suffix specific to the class.
    :return:
    """

    if check_indices_name_valid_for_delete(indices, envId, classSuffix):
        es = get_es_conn()
        es.indices.delete(indices)
    else:
        raise ESInvalidIndexName(indices)


def get_multi_indexes_small_search_query(indices: str, matchFields: dict = {}, termFields: dict = {},
                                         returnFields: List = None, filterMatch: dict = {}, filterTerms: dict = {}):
    """
    Search used to retrieve a reasonable amount of documents (All docs stored in memory, no streaming).):

    :param indices:         List of index names separated by a comma
    :param matchFields:     Uses match in elastic search query context. (This will use any analysers associated with the field).
                                (Ie if you search for value "car", and a document with value "Red car" exists. It will return it.
    :param termFields:      Uses term in elastic search query context. Exact term must exist.
                                (Ie if you search for value "car", and a document with value "Red car" exists. It will NOT return it.
    :param returnFields:    Returns the fields specified in the list. If None, return all fields. It will always return type and id,
                                independent on filters.
    :param filterMatch:     Use match in elastic search filter context. Exact term must exist. Quicker than matchFields, but doesnt use a score.
    :param filterTerms:     Use term in elastic search filter context. Exact term must exist. Quicker than termFields, but doesnt use a score.
    :return:    Query object for making the search
    """
    es = get_es_conn()
    s = Search(using=es, index=indices)

    filterParts = []
    normalParts = []

    if filterTerms:
        add_terms(filterParts, filterTerms)

    if filterMatch:
        for match in filterMatch:
            matchParam = {match: filterMatch[match]}
            filterParts.append(Q("match", **matchParam))

    if termFields:
        add_terms(normalParts, termFields)

    if matchFields:
        for match in matchFields:
            matchParam = {match: matchFields[match]}
            normalParts.append(Q("match", **matchParam))

    if filterParts and normalParts:
        s.query = Q('bool', must=normalParts, filter=filterParts)
    elif filterParts:
        s.query = Q('bool', filter=filterParts)

    elif normalParts:
        s.query = Q('bool', must=normalParts)

    if returnFields is not None:
        s = s.source(returnFields)

    return s


def add_terms(parts, terms):
    for term in terms:
        termValue = terms[term]
        query = term_query(termValue)
        termParam = {term: termValue}
        parts.append(Q(query, **termParam))


def term_query(value):
    if isinstance(value, list):
        return "terms"
    else:
        return "term"


def multi_indexes_small_search(indices: str, matchFields: dict = {}, termFields: dict = {},
                               returnFields: List = None, filterMatch: dict = {}, filterTerms: dict = {}, useScan=True):
    """
    Search used to retrieve a reasonable amount of documents (All docs stored in memory, no streaming).)

    Note: the use of scan prevents sorting. Also, scan is deprecated since version 2.1:
          https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-search-type.html#scan
          But possibly the DSL uses scan or scroll accordingly.

    :param indices:         List of index names separated by a comma
    :param matchFields:     Uses match in elastic search. (This will use any analysers associated with the field).
                                (Ie if you search for value "car", and a document with value "Red car" exists. It will return it.
    :param termFields:      Uses term in elastic search. Exact term must exist.
                                (Ie if you search for value "car", and a document with value "Red car" exists. It will NOT return it.
    :param returnFields:    Returns the fields specified in the list. If None, return all fields. It will always return type and id,
                                independent on filters.
    :param filterMatch:     Use match in elastic search filter context. Exact term must exist. Quicker than matchFields, but doesnt use a score.
    :param filterTerms:     Use term in elastic search filter context. Exact term must exist. Quicker than termFields, but doesnt use a score.
    :param useScan:         If True, uses scroll API to return all results. Howhever only 1 scroll can exist per index currently.
            If false uses search api, which returns at most 10 0000 results.
    :return:
    """

    s = get_multi_indexes_small_search_query(indices, matchFields, termFields, returnFields, filterMatch, filterTerms)
    resultingDocs = []

    def create_doc(res):
        # Convert result to dictionary
        doc = {}
        doc = res.__dict__['_d_']
        doc["id"] = res.meta.id
        doc["type"] = res.meta.doc_type
        return doc

    if useScan:
        s = s.params(scroll=get_scan_scroll_duration(), size=get_nb_documents_per_scan_scroll())
        for res in s.scan():
            resultingDocs.append(create_doc(res))
    else:
        for res in s[0:(MAX_SMALL_SEARCH_NO_SCAN_RESULTS - 1)]:
            resultingDocs.append(create_doc(res))

    return resultingDocs


def check_indices_name_valid_for_delete(indices: str, envId: str, classPrefix: str) -> bool:
    """
    Check if indices names are valid. May be one or multiple indices.

    :param indices:         One index, multiple indeces separated by coma. One index may represent multiple by using star at the end
    :param envId:          envId name
    :param classPrefix:     Suffix specific to the class.

    :return:                False if the name is invalid
    """

    if ',' in indices:  # more then one index entry
        if '*' in indices:
            return False
        else:
            indicesList = indices.split(',')
            for index in indicesList:
                if len(index) == 0:
                    return False
                if not _is_index_name_valid_for_delete(index, envId, classPrefix):
                    return False

            return True
    else:  # one index entry
        if indices.endswith('*'):  # generic. * must be at the end
            return _is_index_name_valid_for_delete(indices[:-1], envId, classPrefix)
        else:
            return _is_index_name_valid_for_delete(indices, envId, classPrefix)


def _is_index_name_valid_for_delete(index: str, envId: str, classPrefix: str):
    if index.startswith(envId + classPrefix):
        if re.match(r'^[\w\-]+$', index):
            return True
    return False


def check_index_name_valid_for_create(index: str, envId: str, classSuffix: str) -> bool:
    if index.startswith(envId + classSuffix) and len(index) > len(envId + classSuffix):
        if re.match(r'^[\w\-]+$', index):
            return True
    return False


def convert_datetime_to_es(dateTime: datetime):
    return "{0}-{1}-{2} {3}:{4}:{5}".format(dateTime.year, dateTime.month,
                                            dateTime.day, dateTime.hour,
                                            dateTime.minute, dateTime.second)


def convert_es_date_to_datetime(esDate: str):
    return datetime.strptime(esDate, ES_TO_DATETIME_DATE_FORMAT)

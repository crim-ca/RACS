from typing import List, Dict
from datetime import datetime
from tornado.web import RequestHandler
from jassrealtime.core.settings_utils import get_jass_allow_cors
from jassrealtime.core.esutils import string_to_id

def missing_fields_message(entity: Dict, requiredFields: List):
    """
    Returns a message if requiredFields are missing in entity

    :param entity:  Dictionary containing keys to be checked
    :param requiredFields:  List of keys which should be present in entity
    :return: a message specifying what required fields are missing. If all required fields
        are present, returns None

    """
    missingFields = []
    for field in requiredFields:
        if field not in entity:
            missingFields.append(field)

    if missingFields:
        return "Missing required fields: " + ",".join(missingFields)
    else:
        return None


def is_missing_required_fields(entity: Dict, requiredFields: List):
    """
    Returns true if requiredFields are missing in entity

    :param entity:  Dictionnary containing keys to be checked
    :param fields:  List of keys which should be present in entity
    :return: true if requiredFields are missing in entity

    """
    for field in requiredFields:
        if not field in entity:
            return True

    return False


def add_cors(requestHandler: RequestHandler):
    """
    Allow CORS requests

    :param requestHandler:
    :return:
    """
    if get_jass_allow_cors:
        requestHandler.set_header("Access-Control-Allow-Origin", "*")
        requestHandler.set_header("Access-Control-Allow-Headers", "x-requested-with")
        requestHandler.set_header('Access-Control-Allow-Methods', 'POST, PUT, DELETE, GET, OPTIONS')


def datetime_to_json_str(dateTime: datetime):
    return dateTime.strftime('%Y-%m-%dT%H:%M:%SZ')


def valid_es_id(id: str):
    """
    This function validates if the id is valid and can be used in an elastic search index name.

    :param id: Id to validate.
    :return: True if id is valid, false otherwise.
    """
    return (id == string_to_id(id))
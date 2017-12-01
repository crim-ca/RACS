# coding: utf-8

import logging
import uuid
import time
from elasticsearch import exceptions

from jassrealtime.core.utils import gen_uuid
from .esutils import *
from ..security.base_authorization import *
from .master_factory_list import create_all_lists_for_env
from .settings_utils import get_number_of_replicas,get_number_of_shards

ENV_MAPPING = {
    "mappings": {
        "default": {
            "properties": {
                "name": {"type": "string"},
            }
        }
    }
}


class EnvNotFoundException(Exception):
    pass


class EnvAlreadyExistWithSameIdException(Exception):
    pass


class EnvList:
    @staticmethod
    def initialize_env_list(sett: dict):
        """
        Initializes the list of environments, if it doesnt exists.
        Should not be user accessible
        :param self:
        :param sett:
        :return:
        """
        es = get_es_conn()
        envIndex = sett['MASTER_ENV_ID']
        if not es.indices.exists(index=envIndex):
            body = ENV_MAPPING
            body["settings"]={}
            body["settings"]["index"] = {"number_of_shards": get_number_of_shards(),
                                         "number_of_replicas": get_number_of_replicas()}

            es.indices.create(index=envIndex,body=body)

    def __init__(self, sett: dict, authorization: BaseAuthorization):
        self.masterenvId = sett['MASTER_ENV_ID']
        self.envIndex = self.masterenvId
        self.authorization = authorization

    def create_env(self, id: str = None, name: str = None):
        """
        TODO: Verify the name correctly

        :param id: Alphanumeric. If not presented the system will generate one.
        :param name: Name to give to the env. Not used ATM
        :return:
        """
        self.authorization.can_create_env()
        es = get_es_conn()
        # TODO validate that you can not create 2 envs with the same name.
        idName = None
        if id:
            try:
                idName = id
                res = es.get(index=self.envIndex, doc_type="default", id=idName)
                raise EnvAlreadyExistWithSameIdException("Id: {0}".format(idName))
            except exceptions.NotFoundError:
                pass
        else:
            idName = gen_uuid()

        es.create(index=self.envIndex, doc_type="default", id=idName, body={"name": name})

        # create corpuses
        create_all_lists_for_env(idName, self.authorization)

        return Env(idName, name)

    def get_env(self, id: str) -> 'Env':
        self.authorization.can_access_env()
        es = get_es_conn()
        es_wait_ready()
        try:
            res = es.get(index=self.envIndex, doc_type="default", id=id)
            return Env(id, res["_source"]["name"])

        except exceptions.NotFoundError:
            raise EnvNotFoundException("Id : {0}".format(id))

    def delete_env(self, id):
        es = get_es_conn()
        self.authorization.can_delete_env()
        env = self.get_env(id)
        es_wait_ready()
        indicesToDelete = env.id + "*"
        es.delete(index=self.envIndex, doc_type="default", id=env.id)
        es.indices.delete(index=indicesToDelete)


class Env:
    """
    Encapsulates a env

    """

    def __init__(self, id: str, name: str):
        """
        Creates a new env

        :param id:
        :param name:
        """
        self.id = id
        self.name = name

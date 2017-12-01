import json
import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_env_list
from jassrealtime.core.env import EnvAlreadyExistWithSameIdException, EnvNotFoundException
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *


class EnvFolderHandler(BaseHandler):
    def post(self):
        try:
            body = json.loads(self.request.body.decode("utf-8"))
            envId = None
            envName = None
            if "id" in body:
                envId = body["id"]
            if "name" in body:
                envName = body["name"]
            # TODO replace by true authorization
            authorization = get_autorisation(envId, None, None)
            env = get_env_list(authorization).create_env(envId, envName)
            self.write_and_set_status({"id": env.id, "name": env.name, "securityType": "basic"},
                                      HTTPStatus.OK)
        except EnvAlreadyExistWithSameIdException:
            self.write_and_set_status({MESSAGE: "env with the same id already exists"},
                                      HTTPStatus.CONFLICT)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)


class EnvHandler(BaseHandler):
    def options(self, envId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, envId):
        try:
            authorization = get_autorisation(envId, None, None)
            env = get_env_list(authorization).get_env(envId)
            self.write_and_set_status(json.dumps({"id": env.id, "name": env.name, "securityType": "basic"}),
                                      HTTPStatus.OK)
        except EnvNotFoundException:
            self.write_and_set_status({MESSAGE: "env with id : {0} doest exists".format(envId)},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def delete(self, envId):
        try:
            authorization = get_autorisation(envId, None, None)
            env = get_env_list(authorization).delete_env(envId)
            self.write_and_set_status(None,
                                      HTTPStatus.NO_CONTENT)
        except EnvNotFoundException:
            self.write_and_set_status({MESSAGE: "env with id : {0} doest exists".format(envId)},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

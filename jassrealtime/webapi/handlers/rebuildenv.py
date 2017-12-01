import json
import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_env_list
from jassrealtime.core.env import EnvAlreadyExistWithSameIdException, EnvNotFoundException
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.core.esutils import es_wait_ready
from time import sleep


class RebuildEnvHandler(BaseHandler):
    def post(self):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            envList = get_env_list(authorization)
            env = envList.get_env(envId)
            envList.delete_env(env.id)
            es_wait_ready()
            sleep(5)
            env = get_env_list(authorization).create_env(env.id, env.name)
            es_wait_ready()
            self.write_and_set_status(None,
                                      HTTPStatus.OK)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write({MESSAGE: "Internal server error", TRACE: trace},
                       HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

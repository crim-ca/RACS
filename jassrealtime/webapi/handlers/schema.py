import json
import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_schema_list
from jassrealtime.core.schema_list import JsonSchemaAlreadyExistsException, JsonSchemaDoesntExistException
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id

class SchemaHandler(BaseHandler):
    def post(self):
        try:
            body = json.loads(self.request.body.decode('utf-8'))
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            schemaDescription = None

            if "id" in body:
                schemaId = body["id"]
            if "name" in body:
                schemaName = body["name"]
            if "description" in body:
                schemaName = body["description"]
            if not "jsonSchema" in body:
                self.write_and_set_status({MESSAGE: "Missing jsonSchema"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            id = get_schema_list(envId, authorization).add_json_schema(body["jsonSchema"],
                                                                          schemaName, schemaDescription, schemaId)
            self.write_and_set_status({"id": id},
                                      HTTPStatus.OK)
        except JsonSchemaAlreadyExistsException:
            self.write_and_set_status({MESSAGE: "Schema with the same id already exists"},
                                      HTTPStatus.CONFLICT)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            schemaId = self.get_argument("id", None)
            res = []
            if schemaId:
                info = get_schema_list(envId, authorization).get_json_schema_info(schemaId)
                res = [info]
            else:
                res = get_schema_list(envId, authorization).get_json_schemas_infos(
                    self.get_argument("name", None), self.get_argument("description", None),
                    self.get_argument("jsonSchemaHash", None), self.get_argument("esHash", None))
            self.write_and_set_status({"data": res},
                                      HTTPStatus.OK)
        except JsonSchemaDoesntExistException:
            self.write_and_set_status({MESSAGE: "There is no schema with the given id"},
                                      HTTPStatus.CONFLICT)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

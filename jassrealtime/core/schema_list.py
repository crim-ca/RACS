# coding: utf-8

import hashlib
import json

from jassrealtime.core.utils import gen_uuid
from .esutils import *
from ..security.base_authorization import *
from .language_manager import LanguageManager
from uuid import uuid1
from .settings_utils import get_number_of_replicas,get_number_of_shards

from elasticsearch import exceptions
from elasticsearch_dsl import Q
from elasticsearch_dsl import Search

MASTER_JSON_SCHEMA_INDEX_ES_MAPPING = {
    "mappings": {
        "default": {
            "properties": {
                "jsonSchemaHash": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "esHash": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "jsonSchema": {
                    "type": "string",
                    "index": "no"
                },
                "name": {
                    "type": "string"
                },
                "description": {
                    "type": "string"
                }
            }
        }
    }
}

MASTER_ES_SCHEMA_INDEX_MAPPING = {
    "mappings": {
        "default": {
            "properties": {
                "esSchema": {
                    "type": "string",
                    "index": "no"
                }
            }
        }
    }
}

JSON_SCHEMA_TYPE_TO_ES_TYPE = {"boolean": "boolean", "long": "long", "integer": "long", "number": "double",
                               "string": "string"}

JSON_SCHEMA_PRIMITIVE_TYPES = {"boolean", "integer", "number", "string"}


class SchemaException(Exception):
    pass


class SchemaBindingInvalid(SchemaException):
    pass


class InvalidLanguageException(SchemaException):
    pass


class JsonSchemaAlreadyExistsException(SchemaException):
    pass


class JsonSchemaDoesntExistException(SchemaException):
    pass


class EsSchemaDoesntExist(SchemaException):
    pass


class EsSchemaMigrationInvalidException(SchemaException):
    pass


class EsSchemaNotSupportedProperty(SchemaException):
    pass


class EsSchemaMigrationDeleteFieldsNotSupportedException(SchemaException):
    pass


class SchemaList:
    def __init__(self, envId: str, sett: dict, authorization: BaseAuthorization, languageManager: LanguageManager):
        """
        Creates a schema list envId.
        PRECONDITION: envId must be at least 3 characters long.

        :param envId: ALPHANUMERIC (With underscores) string representing a envId.
        :param sett:  Settings object containing specific settings.
        """
        self.envId = envId
        self.classIndex = sett['CLASS_PREFIX']
        self.authorization = authorization
        self.masterJsonSchemaIndex = self.envId + self.classIndex + sett['MASTER_JSON_SCHEMA_SUFFIX']
        self.masterEsSchemaIndex = self.envId + self.classIndex + sett['MASTER_ES_SCHEMA_SUFFIX']
        self.languageManager = languageManager

    @staticmethod
    def create(envId, sett: dict, authorization: BaseAuthorization, languageManager: LanguageManager):
        """
        This function will create lists useful to store schemas and bindings

        :return:
        """
        authorization.can_create_env()

        schemaList = SchemaList(envId, sett, authorization, languageManager)
        es = get_es_conn()
        es_wait_ready()
        if check_index_name_valid_for_create(schemaList.masterJsonSchemaIndex, schemaList.envId,
                                             schemaList.classIndex):
            body = MASTER_ES_SCHEMA_INDEX_MAPPING
            body["settings"] = {}
            body["settings"]["index"] = {"number_of_shards": get_number_of_shards(),
                                         "number_of_replicas": get_number_of_replicas()}

            es.indices.create(index=schemaList.masterEsSchemaIndex, body=body)
        else:
            raise ESInvalidIndexName(schemaList.masterJsonSchemaIndex)

        es_wait_ready()
        if check_index_name_valid_for_create(schemaList.masterJsonSchemaIndex, schemaList.envId,
                                             schemaList.classIndex):
            body = MASTER_JSON_SCHEMA_INDEX_ES_MAPPING
            body["settings"] = {}
            body["settings"]["index"] = {"number_of_shards": get_number_of_shards(),
                                         "number_of_replicas": get_number_of_replicas()}
            es.indices.create(index=schemaList.masterJsonSchemaIndex, body=body)
        else:
            raise ESInvalidIndexName(schemaList.masterJsonSchemaIndex)

        return schemaList

    @staticmethod
    def hash_json_schema(jsonSchema: dict):
        """
        Creates a unique hash for json schema.

        :param jsonSchema:
        :return:
        """

        jsonToString = SchemaList.__convert_nested_dict_to_string(jsonSchema)
        # added h to make sure we have a hash which can be used as id in elastic search.
        return "h" + hashlib.sha1(jsonSchema.encode("utf8")).hexdigest()

    @staticmethod
    def hash_es_properties(esProperties: dict):
        """
        Converts elastic search properties to a unique hash. Note that esMapping should not be type dependant.

        :param      esProperties:       Properties in the mapping section
        :return:    esHash. A unique hash produced by this schema.
        """
        propertiesToString = SchemaList.__convert_nested_dict_to_string(esProperties)
        return hashlib.sha1(propertiesToString.encode("utf8")).hexdigest()

    @staticmethod
    def __convert_nested_dict_to_string(nestedDict) -> str:
        """
        This will transmorm a nested Dictionnary to an ordered string. Will sort dictionnary by key
        and arrays of primitive types.

        :param nestedDict:
        :return:    returns the result as and ordered string
        """

        if isinstance(nestedDict, list):
            l = []
            for ele in nestedDict:
                l.append(SchemaList.__convert_nested_dict_to_string(ele))
            return "".join(sorted(l))
        elif isinstance(nestedDict, dict):
            l = []
            sortedKeys = sorted(nestedDict)
            for key in sortedKeys:
                res = str(key) + SchemaList.__convert_nested_dict_to_string(nestedDict[key])
                l.append(res)
            return "".join(sorted(l))
        else:
            return str(nestedDict)

    @staticmethod
    def can_migrate(esPropertiesFrom: dict, esPropertiesTo: dict):
        """
        Returns true if you can move from one esSchema to another. This does not mean that the new Json Schema will
        be respected.

        :param esPropertiesFrom:      initial properties
        :param esPropertiesTo:        properties to migrate to
        :return:
        """

        logger = logging.getLogger(__name__)
        es = get_es_conn()

        fromProperties = esPropertiesFrom["properties"]
        toProperties = esPropertiesTo["properties"]

        for property in fromProperties:
            if property in toProperties:
                if not is_nested_dict_equal(fromProperties[property], toProperties[property]):
                    message = "Incompatible Mappings for property: existing {0}" \
                        .format(property)
                    logger.info(message)
                    return False

        for property in fromProperties:
            if not property in toProperties:
                raise EsSchemaMigrationDeleteFieldsNotSupportedException(property)

        return True

    @staticmethod
    def calculate_properties_delta(esPropertiesFrom: dict, esPropertiesTo: dict):
        if (SchemaList.can_migrate(esPropertiesFrom, esPropertiesTo)):
            fromProperties = esPropertiesFrom["properties"]
            toProperties = esPropertiesTo["properties"]

            newProperties = {}
            for property in toProperties:
                if not property in fromProperties:
                    newProperties[property] = toProperties[property]

            return {"properties": newProperties}
        else:
            raise EsSchemaMigrationInvalidException

    def delete(self):
        """
        This function will destroy all schemas and bindings.

        :return:
        """
        self.authorization.can_delete_env()
        es = get_es_conn()
        es_wait_ready()
        delete_indices(self.masterEsSchemaIndex, self.envId, self.classIndex)
        es_wait_ready()
        delete_indices(self.masterJsonSchemaIndex, self.envId, self.classIndex)

    def add_json_schema_as_hash(self, jsonSchema: dict, shoudlValidate: bool = False, nestedFields=[]) -> str:
        """
        Add this json schema and uses jsonSchemaHash as id.
        If a schema with the same jsonHash already exists does nothing

        :param jsonSchema:
        :param shouldValidate:      If true it will validate the schema before adding.
        :param nestedFields:    Lists arrays which should be considered as nested properties in es

        return: Id of the added schema (or existing if same)
        """
        self.authorization.can_add_json_schema()
        jsonSchemaStr = json.dumps(jsonSchema)
        jsonSchemaHash = SchemaList.hash_json_schema(jsonSchemaStr)
        try:
            id = self.add_json_schema(jsonSchema, None, None, jsonSchemaHash, shoudlValidate, nestedFields)
            return id
        except JsonSchemaAlreadyExistsException:
            return jsonSchemaHash

    def add_json_schema(self, jsonSchema: dict, name: str = None,
                        description: str = None, id: str = None, shoudlValidate: bool = False,
                        nestedFields=[]):
        """
        This function is used by the user to add his json schema, and able to search it.
        A json schema added by the system will be automatically searchable by hash.

        :param authorization:       User authorization object
        :param jsonSchema:          Json Schema
        :param name:                Optional name used by the user to search for the schema
        :param description:         Optional description used by the user to search for the shema
        :param id:                  Id to uniquely identify this JsonSchema
        :param shouldValidate:      If true it will validate the schema before adding.
        :param nestedFields:    Lists arrays which should be considered as nested properties in es
        :return:                    jsonSchemaInfoId: used to get the information about json schema
        """

        self.authorization.can_add_json_schema()
        jsonSchemaStr = json.dumps(jsonSchema)
        jsonSchemaHash = SchemaList.hash_json_schema(jsonSchemaStr)
        esProperties = self.convert_json_schema_to_es_properties(jsonSchema, nestedFields)
        esHash = self.add_es_schema(esProperties)

        tmpName = ""
        tmpDescription = ""
        entry = {}
        if name:
            tmpName = name
            entry["name"] = name
        if description:
            tmpDescription = description
            entry["description"] = description

        es = get_es_conn()
        if id:
            try:
                es.get(index=self.masterJsonSchemaIndex, doc_type="default", id=id)
                raise JsonSchemaAlreadyExistsException("Schema with same id already exist: {0}".format(id))
            except exceptions.NotFoundError:
                pass
        else:
            id = gen_uuid()

        if shoudlValidate:
            raise NotImplemented()

        entry["jsonSchemaHash"] = jsonSchemaHash
        entry["esHash"] = esHash
        entry["jsonSchema"] = json.dumps(jsonSchema)
        es.create(index=self.masterJsonSchemaIndex, doc_type="default", id=id, body=entry)

        return id

    def get_json_schemas_infos(self, name: str = None, description: str = None, jsonSchemaHash=None, esHash=None):
        """
        Returns json schema information, based on the search criterias.
        Returned infromation:
        :param name:
        :param description:
        :param jsonSchemaHash:
        :param esHash:
        :return:
        """
        queryParts = []
        if name:
            queryParts.append(Q("match", name=name))
        if description:
            queryParts.append(Q("match", description=description))
        if jsonSchemaHash:
            queryParts.append(Q("term", jsonSchemaHash=jsonSchemaHash))
        if esHash:
            queryParts.append(Q("term", esHash=esHash))

        es = get_es_conn()
        s = Search(using=es, index=self.masterJsonSchemaIndex, doc_type="default")
        if len(queryParts) > 0:
            s.query = Q('bool', must=queryParts)
        executedQuery = s.execute()

        resultingDocs = []
        for res in executedQuery:
            # Convert result to dictionary
            doc = {}
            doc = res.__dict__['_d_']
            doc["id"] = res.meta.id
            doc["jsonSchema"] = json.loads(doc["jsonSchema"])
            resultingDocs.append(doc)

        return resultingDocs

    def get_json_schema_info(self, jsonSchemaId: str):
        """
        Returns the info for one json schema

        :param jsonSchemaId:
        :return:
        """
        es = get_es_conn()
        try:
            res = es.get(index=self.masterJsonSchemaIndex, doc_type="default", id=jsonSchemaId)
            doc = res['_source']
            doc["id"] = jsonSchemaId
            doc["jsonSchema"] = json.loads(res["_source"]["jsonSchema"])
            return doc
        except exceptions.NotFoundError:
            raise JsonSchemaDoesntExistException(jsonSchemaId)

    def convert_json_schema_to_es_properties(self, jsonSchema: dict, nestedFields=[]):
        """
        Converts a valid jsonSchema to elastic search mapping.
        Note that the mapping is NOT doctype dependant.

        :param jsonSchema:
        :param nestedFields: Considers a json property of type array of objects as a nested object.
        :return:
        """
        schemaProp = jsonSchema["properties"]
        # We don't really care if it is an array, if all the types are same.

        properties = {"properties": {}}
        for key in schemaProp:
            item = schemaProp[key]
            properties["properties"][key] = {}
            if item["type"] == "array":
                #  Consider putting this case in it's own function
                if "items" not in item:
                    raise EsSchemaNotSupportedProperty("Missing items field for {0}".format(key))

                itemsData = item["items"]
                if key in nestedFields:
                    # Here we make up an ES mapping of nested objects

                    if itemsData["type"] != "object":
                        raise EsSchemaNotSupportedProperty("Type of nested object not supported".format(key))

                    else:
                        if "properties" not in itemsData:
                            raise EsSchemaNotSupportedProperty("Missing properties field for {0}".format(key))

                        nestedProperties = {}
                        for nestedProperty in itemsData["properties"]:
                            # Only keep type for property. No analysers
                            nestedProperties[nestedProperty] = {}
                            if "type" not in itemsData["properties"][nestedProperty]:
                                raise EsSchemaNotSupportedProperty(
                                    "Items of nested properties need a type {0}".format(key))

                            nestedProperties[nestedProperty]["type"] = JSON_SCHEMA_TYPE_TO_ES_TYPE[
                                itemsData["properties"][nestedProperty]["type"]]
                        # update the nested properties
                        properties["properties"][key]["type"] = "nested"
                        properties["properties"][key]["dynamic"] = "strict"
                        properties["properties"][key]["properties"] = nestedProperties

                else:
                    # Accept array properties of primitive types or flat object but do not treat then as nested objects
                    if "type" not in itemsData:
                        raise EsSchemaNotSupportedProperty("Missing type field for {0}".format(key))

                    jsonType = itemsData["type"]

                    # Array of primitive types (must all be the same type)
                    if jsonType in JSON_SCHEMA_PRIMITIVE_TYPES:
                        properties["properties"][key]["type"] = JSON_SCHEMA_TYPE_TO_ES_TYPE[jsonType]

                    # Array of flat object (must all be the same object)
                    # Here, we make a normal ES object mapping
                    elif jsonType == "object":
                        if "properties" not in itemsData:
                            raise EsSchemaNotSupportedProperty("Missing properties field for {0}".format(key))
                        raise NotImplementedError(
                            "Properties of type array with items of type object not supported yet.")

                    else:
                        raise EsSchemaNotSupportedProperty(key)
            else:  # Not an array
                properties["properties"][key] = self.atomic_field_mapping(item, key)

        return properties

    def atomic_field_mapping(self, item, key):
        field_type = JSON_SCHEMA_TYPE_TO_ES_TYPE[item["type"]]
        if "searchable" not in item:  # searchable is not present in the schema
            mapping = self.mapping_field_not_indexed(field_type)
        else:
            if not item["searchable"]:  # searchable IS present, but value is considered false
                mapping = self.mapping_field_not_indexed(field_type)
            else:
                if "searchModes" not in item:
                    raise SchemaBindingInvalid("searchModes missing for field " + key)
                else:
                    search_modes = item["searchModes"]

                    if type(search_modes) is not list:
                        raise SchemaBindingInvalid("searchModes must be a list for field " + key)

                    # Special case: default index
                    language = item.get("language")
                    mapping = self.mapping_field_index(search_modes[0], field_type, language)

                    # Subsequent cases: index names will be appended with the corresponding searchMode.
                    # E.g. title.edge
                    subsequent_indices = search_modes[1:]
                    if subsequent_indices:
                        mapping["fields"] = {}
                        for search_mode in subsequent_indices:
                            sub_mapping = self.mapping_field_index(search_mode, field_type, language)
                            mapping["fields"][search_mode] = sub_mapping
        return mapping

    @staticmethod
    def make_not_indexed(key, properties):
        properties["properties"][key]["index"] = "no"

    def add_es_schema(self, esProperties: dict):
        """
        Add an esScheam to the list of available schemas. (If already exists do nothing)

        :param esProperties:      Properties Section of es schema
        :return:                  esHash
        """
        esHash = SchemaList.hash_es_properties(esProperties)
        schemaDump = json.dumps(esProperties)
        es = get_es_conn()
        try:
            es.get(index=self.masterEsSchemaIndex, doc_type="default", id=esHash)
        except exceptions.NotFoundError:
            es.create(index=self.masterEsSchemaIndex, doc_type="default", body={"esSchema": schemaDump}, id=esHash)

        return esHash

    def get_es_properties(self, esHash: str):
        """
        Returns esProprieties associated with esHash

        :param esHash:
        :return:
        """
        es = get_es_conn()
        try:
            propertiesInfo = es.get(index=self.masterEsSchemaIndex, doc_type="default", id=esHash)
            esProperties = json.loads(propertiesInfo["_source"]["esSchema"])
            return esProperties
        except exceptions.NotFoundError:
            raise EsSchemaDoesntExist(esHash)

    @staticmethod
    def mapping_field_index(search_mode: str, field_type: str, language: str) -> dict:
        mapping = {"type": field_type}

        if search_mode == "noop":
            mapping["index"] = "not_analyzed"
        elif search_mode == "basic":
            mapping["analyzer"] = "standard"
        elif search_mode == "edge":
            mapping["index"] = "analyzed"
            mapping["analyzer"] = "autocomplete"
            mapping["search_analyzer"] = "autocomplete_search"
        elif search_mode == "path":
            mapping["index"] = "analyzed"
            mapping["analyzer"] = "path_analyzer"
        elif search_mode == "ngram":
            mapping["index"] = "analyzed"
            mapping["analyzer"] = "ngram_filter_analyzer"
        elif search_mode == "language":
            if not language:
                raise SchemaBindingInvalid("Missing attribute language for searchMode '{0}'.".format(search_mode))
            mapping["analyzer"] = language
        else:
            raise SchemaBindingInvalid("searchMode '{0}' invalid".format(search_mode))

        return mapping

    @staticmethod
    def mapping_field_not_indexed(field_type: str) -> dict:
        return {"index": "no", "type": field_type}

# coding: utf-8
from builtins import staticmethod


class SecurityToken:
    """
    This class represents an INTERNAL security token. Containing the information necessary to the security module.
    """

    def __init__(self, userId=None, originalToken=None):
        """
        Initializes the token with the original token.
        """
        self.userId = userId
        self.originalToken = originalToken
        self.properties = {}

    def add_property(self, propertyName: str, propertyValue: str):
        """
        Adds a custom property to the token. This custom property may be needed by a specific autorisation class.
        If property with the same name exists, overwrites it.

        :param propertyName: Name of the property.Must be an alphanumeric string describing.
        :param propertyName:string/number/object
        :return:
        """

        self.properties[propertyName] = propertyValue

    def get_property(self, propertyName: str):
        """
        Returns a propertyValue.
        Throws an exception if property is not found.

        :param propertyName:  Name of the property.Must be an alphanumeric string describing.
        :return: PropertyValue associated.
        """

        return self.properties[propertyName]

    def property_exist(self, propertyName: str):
        """
        Throws an exception if the property exist. Property must be a valid alphanumeric string.

        :param propertyName: Name of the property.Must be an alphanumeric string describing.
        :return:
        """

        return (propertyName in self.properties)


class InvalidToken(Exception):
    pass


class InvalidTokenType(Exception):
    pass


class PermissionDenied(Exception):
    pass


class BaseAuthorization:
    """
    Basic authorisation class. Should not be used directly, but subclassed.
    This class allows ALL.

    """

    @staticmethod
    def create_authorization(envId, userToken: object, tokenType: str):
        """
        Attempts to the authorisation class. If the supplied info is invalid will throw an exception.

        :param userToken: token supplied by the user.
        :param tokenType: describes the type of token for extraction.
        """
        # raise InvalidToken

        return BaseAuthorization(envId, object, tokenType, None)

    def __init__(self, envId, userToken: object, tokenType: str, securityToken: SecurityToken):
        """
        :param userToken: token supplied by the user.
        :param tokenType: describes the type of token for extraction.
        :param securityToken:   extra info provided from the token
        """

        self.userToken = userToken
        self.tokenType = tokenType
        self.tokenType = securityToken
        self.envId = envId

    def can_create_env(self):
        """
        Throws an exception if the user is not allowed to create a envId

        :return:
        """
        pass

    def can_delete_env(self):
        """
        Throws an exception if the user is not allowed to delete a envId

        :return:
        """
        pass

    def can_access_env(self):
        """
        Throws an exception if the user is not allowed to access a envId

        :return:
        """
        pass

    ##### DocumentCorpus

    def can_create_document_corpus(self):
        """
        Throws an exception if the user is not allowed to create a corpus

        :return:
        """
        pass

    def can_get_document_corpus(self, corpusId: str):
        """
        Throws an exception if the user can not get the document corpus

        :return:
        """
        pass

    def can_delete_document_corpus(self, corpusId: str):
        """
        Throws an exception if user is not not allowed to delete a corpus

        :return:
        """
        pass

    def can_add_document_to_corpus(self, corpusId: str):
        """
        Throws an exception if user is not not allowed to add a document to corpus

        :return:
        """
        pass

    def can_read_document_from_corpus(self, corpusId: str):
        """
        Throws an exception if user is not not allowed to read a document from sub corpus.

        :return:
        """

        pass

    def list_document_corpuses_id(self):
        """
        Returns the id of all corpuses the user is allowed to see.
        :return:
        """
        return []

    ##### DocumentSubCorpus

    def can_create_document_sub_corpus(self, corpusId):
        """
        Throws an exception if user is not allowed to create a sub corpus

        :return:
        """
        pass

    def can_delete_document_sub_corpus(self, corpusId, subCorpusId):
        """
        Throws an exception if user is not allowed to delete a sub corpus

        :return:
        """
        pass

    def can_read_document_sub_corpus(self, corpusId, subCorpusId):
        """
         Throws an exception if user is not allowed to read a document from sub corpus

         :return:
         """
        pass

    def can_add_document_id_to_sub_corpus(self, corpusId, subCorpusId):
        """
        Throws an exception if user can add a document reference to sub corpus
        :return:
        """
        pass

    ##### Bucket

    def can_create_bucket(self, corpusId):
        """
        Throws an exception if user is not allowed to create a bucket for a given corpus

        :return:
        """
        pass

    def can_delete_bucket(self, corpusId, bucketId):
        """
        Throws an exception if user is not allowed to delete a bucket

        :return:
        """
        pass

    def can_read_bucket(self, corpusId, bucketId):
        """
         Throws an exception if user is not allowed to read an annotation inside bucket, or to read a bucket target.

         :return:
         """
        pass

    def can_change_bucket_target(self, corpusId, bucketId):
        """
        Throws an exception if user is not allowed to change the target of the bucket

        :return:
        """
        pass

    def can_add_annotation(self, corpusId, bucketId):
        """
        Throws an exception if user is not allowed to add or modify an annotation to the bucket

        """
        pass

    def can_update_annotation(self, corpusId, bucketId):
        """
        Throws an exception if user is not allowed to add or modify an annotation to the bucket

        """
        pass

    def can_delete_annotation(self, corpusId, bucketId):
        """
        Throws an exception if user is not allowed to delete an annotation from the bucket

        """
        pass

    def can_add_json_schema_to_bucket(self, corpusId, bucketId):
        """
        Throws an exception if user is not allowed to add a new json schema to bucket.
        This includes modifying an existing json schema

        """
        pass

    # json schema

    def can_add_json_schema(self):
        """
        Throws an exception  if the user can not add a json schema to the system.
        
        :return: 
        """
        pass

    def can_remove_json_schema(self):
        """
        Throws an exception  if the user can not remove a json schema

        :return:
        """
        pass

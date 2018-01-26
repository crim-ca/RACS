# This class is used to as a factory to create various lists used by multiple classes.

from .settings_utils import *
from ..security.base_authorization import *


# Note these functions are used to get various lists.
# It assumes the lists were created beforehand


def get_env_list(authorization: BaseAuthorization):
    """

    :param envId:
    :return:        SchemaList associated with the envId
    """

    from jassrealtime.core.env import EnvList
    sett = get_settings()
    return EnvList(sett['CLASSES']['ENV'], authorization)


def get_master_document_directory_list(envId: str, authorization: BaseAuthorization):
    """

    :param envId:
    :return:        return DocumentDirectoryList associated with the envId
    """
    from jassrealtime.core.document_directory import DocumentDirectoryList
    sett = get_settings()
    return DocumentDirectoryList(envId, sett['CLASSES']['DOCUMENT_DIRECTORY'], authorization)


def get_master_document_corpus_list(envId: str, authorization: BaseAuthorization):
    """

    :param envId:
    :return:        DocumentCorpusList associated with the envId
    """
    from jassrealtime.document.document_corpus import DocumentCorpusList
    sett = get_settings()
    return DocumentCorpusList(envId, sett['CLASSES']['DOCUMENT_CORPUS'], authorization)


def get_master_document_sub_corpus_list(envId: str, authorization: BaseAuthorization):
    """

    :param envId:
    :return:        DocumentCorpusList associated with the envId
    """
    from jassrealtime.document.document_sub_corpus import DocumentSubCorpusList
    sett = get_settings()
    return DocumentSubCorpusList(envId, sett['CLASSES']['DOCUMENT_CORPUS']['DOCUMENT_SUB_CORPUS'], authorization)


def get_master_bucket_list(envId: str, authorization: BaseAuthorization):
    """

    :param envId:
    :return:        DocumentCorpusList associated with the envId
    """
    from jassrealtime.document.bucket import BucketList
    sett = get_settings()
    bucketSettings = sett['CLASSES']['BUCKET']
    bucketMasterDir = get_master_document_directory_list(envId, authorization).get_directory(
        bucketSettings['MASTER_BUCKET_ID'])

    return BucketList(envId, bucketSettings, bucketMasterDir, authorization)


def get_schema_list(envId: str, authorization: BaseAuthorization):
    """

    :param envId:
    :return:        SchemaList associated with the envId
    """

    from jassrealtime.core.schema_list import SchemaList
    sett = get_settings()
    return SchemaList(envId, sett['CLASSES']['SCHEMA_LIST'], authorization, get_language_manager())


def create_all_lists_for_env(envId: str, authorization: BaseAuthorization):
    """
    Creates all necessary lists for a env.

    :return:
    """
    authorization.can_create_env()
    from jassrealtime.core.document_directory import DocumentDirectoryList
    from jassrealtime.document.document_corpus import DocumentCorpusList
    from jassrealtime.document.bucket import BucketList
    from jassrealtime.core.schema_list import SchemaList
    from time import sleep

    # really need to replace timers
    sett = get_settings()
    DocumentDirectoryList.create(envId, sett['CLASSES']['DOCUMENT_DIRECTORY'], authorization)
    sleep(1)
    SchemaList.create(envId, sett['CLASSES']['SCHEMA_LIST'], authorization, get_language_manager())
    sleep(1)
    DocumentCorpusList.create(envId, sett['CLASSES']['DOCUMENT_CORPUS'], authorization)
    sleep(1)
    BucketList.create(envId, sett['CLASSES']['BUCKET'], authorization)
    sleep(1)

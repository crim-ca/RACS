import os

import tornado.ioloop
import tornado.web
import logging
from tornado.httpserver import HTTPServer

from jassrealtime.core.env import EnvList, EnvNotFoundException
from jassrealtime.core.master_factory_list import get_env_list
from jassrealtime.core.settings_utils import can_manage_env, can_rebuild_env, get_settings, \
    get_env_id, get_log_level, get_nb_cores, get_expose_swagger
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.core.esutils import es_wait_ready
from jassrealtime.webapi.handlers.annotation_count import AnnotationCountHandler

from jassrealtime.webapi.handlers.corpus import CorporaHandler
from jassrealtime.webapi.handlers.corpus import CorpusHandler
from jassrealtime.webapi.handlers.env import EnvHandler, EnvFolderHandler
from jassrealtime.webapi.handlers.rebuildenv import RebuildEnvHandler
from jassrealtime.webapi.handlers.bucket import BucketHandler, BucketFolderHandler
from jassrealtime.webapi.handlers.bucket_schema import BucketSchemaHandler,BucketSchemaDeleteHandler
from jassrealtime.webapi.handlers.annotations import AnnotationHandler, AnnotationFolderHandler
from jassrealtime.webapi.handlers.document import DocumentHandler, DocumentFolderHandler, DocumentIdsHandler
from jassrealtime.webapi.handlers.structure import StructureHandler
from jassrealtime.webapi.handlers.search import DocumentSearchHandler, DocumentFolderSearchHandler, \
    SingleTypeDocumentSearchHandler, DocumentMetadataSearchHandler
from jassrealtime.webapi.batch_handlers.batch_annotations import BatchAnnotationsUploadHandler, BatchAnnotationsDownloadHandler
from jassrealtime.webapi.batch_handlers.batch_documents import BatchDocumentsHandler

settings = {}

if get_expose_swagger():
    settings = {
        'debug': True,
        "static_path": os.path.join(os.path.dirname(__file__), "static"),
        "autoreload": False
    }
else:
    settings = {
        'debug': True,
        "autoreload": False
    }

# idsStruct = "(.*)"
idsStruct = "([\w-]+)"
handlers = [
        (r"/corpora/{0}".format(idsStruct), CorpusHandler),
        (r"/corpora", CorporaHandler),
        (r"/corpora/{0}/structure".format(idsStruct), StructureHandler),
        (r"/corpora/{0}/documentIds".format(idsStruct), DocumentIdsHandler),
        (r"/corpora/{0}/documents/{0}".format(idsStruct), DocumentHandler),
        (r"/corpora/{0}/documents".format(idsStruct), DocumentFolderHandler),
        (r"/corpora/{0}/buckets".format(idsStruct), BucketHandler),
        (r"/corpora/{0}/buckets/{0}".format(idsStruct), BucketFolderHandler),
        (r"/corpora/{0}/buckets/{0}/schemas".format(idsStruct), BucketSchemaHandler),
        (r"/corpora/{0}/buckets/{0}/schemas/{0}".format(idsStruct), BucketSchemaDeleteHandler),
        (r"/corpora/{0}/buckets/{0}/annotations/{0}".format(idsStruct), AnnotationHandler),
        (r"/corpora/{0}/buckets/{0}/annotations".format(idsStruct), AnnotationFolderHandler),
        (r"/corpora/{0}/buckets/{0}/annotationCount".format(idsStruct), AnnotationCountHandler),
        (r"/annosearch/corpora/{0}/documents/{0}".format(idsStruct), DocumentSearchHandler),
        (r"/annosearch/corpora/{0}".format(idsStruct), DocumentFolderSearchHandler),
        (r"/annosearch/corpora/{0}/bucket/{0}/schemaType/{0}".format(idsStruct), SingleTypeDocumentSearchHandler),
        (r"/annosearch/documents".format(idsStruct), DocumentMetadataSearchHandler),
        (r"/batch/corpora/{0}/documents".format(idsStruct), BatchDocumentsHandler),
        (r"/batch/corpora/{0}/annotations".format(idsStruct), BatchAnnotationsDownloadHandler),
        (r"/batch/corpora/{0}/bucket/{0}/annotations".format(idsStruct), BatchAnnotationsUploadHandler)
    ]

def make_app():
    setup_logging()
    initialize_es()
    set_up_environnement()

    if can_rebuild_env():
        handlers.append((r"/rebuildenv", RebuildEnvHandler))

    if can_manage_env():
        handlers.append((r"/envs".format(idsStruct), EnvFolderHandler))
        handlers.append((r"/envs/{0}".format(idsStruct), EnvHandler))

    logger = logging.getLogger(__name__)
    logger.info("JASS is READY")
    return tornado.web.Application(handlers, **settings)


def setup_logging():
    logLevelStr = get_log_level()
    logLevel = logging.INFO
    if logLevelStr == "debug":
        logLevel = logging.DEBUG
    elif logLevelStr == "info":
        logLevel = logging.INFO
    elif logLevelStr == "warning":
        logLevel = logging.WARNING
    elif logLevelStr == "error":
        logLevel = logging.ERROR
    else:
        raise ValueError("Invalid log level passed as parameter {0}".format(logLevelStr))

    logging.basicConfig(level=logLevel, handlers=[logging.StreamHandler()],
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logging.getLogger("urllib3").setLevel(logging.ERROR)


def initialize_es():
    """
    Initialise elastic search if required.

    :return:
    """

    sett = get_settings()
    es_wait_ready()
    EnvList.initialize_env_list(sett['CLASSES']['ENV'])


def set_up_environnement():
    try:
        es_wait_ready()
        envId = get_env_id()
        authorization = get_autorisation(envId, None, None)
        envList = get_env_list(authorization)
        envList.get_env(envId)
    except EnvNotFoundException:
        es_wait_ready()
        envList.create_env(envId)


if __name__ == "__main__":
    server =  HTTPServer(make_app())
    server.bind(8888)
    server.start(get_nb_cores())
    tornado.ioloop.IOLoop.current().start()

import traceback
from http import HTTPStatus

from ...webapi.handlers.search_documents import SearchDocumentsHandler
from ...core.settings_utils import get_env_id
from ...security.security_selector import get_autorisation
from ...document.bucket import BucketNotFoundException
from ...document.document_corpus import CorpusNotFoundException
from ...search.multicorpus.multi_corpus import MultiCorpus
from .parameter_names import MESSAGE, TRACE


class SearchDocumentQueryStructureHandler(SearchDocumentsHandler):
    def get(self):
        try:
            # Get corpus id list and their respective bucket ids if any
            targets_argument = self.get_query_argument("targets", default=None)
            if not targets_argument:
                self.missing_required_field("targets")
                return

            targets = self.parse_targets(targets_argument)
            grouped_targets = self.group_targets(targets)

            env_id = get_env_id()
            authorization = get_autorisation(env_id, None, None)
            mc = MultiCorpus(env_id, authorization)
            structure = mc.query_structure(grouped_targets)

            self.write_and_set_status({"structure": structure}, HTTPStatus.OK)
        except CorpusNotFoundException as exception:
            self.write_and_set_status({MESSAGE: "Corpus not found with id:'{}'".format(exception.corpus_id)},
                                      HTTPStatus.NOT_FOUND)
        except BucketNotFoundException as exception:
            self.write_and_set_status({MESSAGE: "Bucket not found with id:'{}'".format(exception.bucket_id)},
                                      HTTPStatus.NOT_FOUND)
        except ValueError as error:
            self.write_and_set_status({MESSAGE: str(error)}, HTTPStatus.BAD_REQUEST)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

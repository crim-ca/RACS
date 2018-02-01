from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler


class SearchDocumentByTextHandler(BaseHandler):
    def data_received(self, chunk):
        pass

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self):
        raise NotImplementedError()


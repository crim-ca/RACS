import json
from codecs import BOM_UTF8
from http import HTTPStatus
from tornado.web import RequestHandler

from jassrealtime.webapi.handlers.parameter_names import MESSAGE
from jassrealtime.webapi.handlers.utils import add_cors


class BaseHandler(RequestHandler):
    """
    Base handler offering some utility methods.
    Should be sub classed by all handlers withing JASS

    """

    def write_and_set_status(self, message: dict, status: HTTPStatus):
        """
        Writes message and set status. Adds coors headers if applies
        :param message:
        :param status:
        :return:
        """
        if message is not None:
            self.write(message)
        add_cors(self)
        self.set_status(status)

    def strip_body_bom(self, bom=BOM_UTF8):
        body = self.request.body
        if body.startswith(bom):
            body = body[len(bom):]

        return json.loads(body.decode("utf-8"))

    def send_zip_file_with_get(self, localFilePath: str, downloadFileName: str = 'default.zip'):
        """
        Sends a local zip file, via get request
        :param localFilePath:
        :param downloadFileName:
        :return:
        """
        buf_size = 4096
        self.set_header('Content-Type', 'application/zip')
        self.set_header('Content-Disposition', 'attachment; filename=' + downloadFileName)
        with open(localFilePath, 'rb') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    break
                self.write(data)
        self.finish()

    def missing_required_field(self, required_field):
        self.write_and_set_status({MESSAGE: "Missing required parameters. {0}".format(required_field)},
                                  HTTPStatus.UNPROCESSABLE_ENTITY)

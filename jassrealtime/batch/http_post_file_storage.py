import os, errno, zipfile, json
from uuid import uuid1
from enum import Enum, unique
from ..core.esutils import get_settings
from ..core.settings_utils import get_jass_tmp_dir
from ..core.utils import utf8_json_dump
import requests
import mmap
from .tmp_file_storage import TmpFileStorage


class UploadUrlFailException(Exception):
    pass

class HttpPostFileStorage(TmpFileStorage):
    """
    This class sends creates a zip and sends it via post to a target url.
    This class assumes full control of the temporary folder : get_jass_tmp_upload_folder()
    """
    tmpFileStorage = None

    def __init__(self, postUrl: str, zipFileName: str = None):
        """
        :param postUrl: Url to post the file to
        :param zipFileName: Name of the zip file containing all the files.
            Needs to be alphanumeric with -_.
        """
        super().__init__(zipFileName)
        self.postUrl = postUrl

    def flush(self, removeEmpty: bool = True,isSendPut = False ,isMultipart: bool = True, multipartFieldName: str = "file"):
        """
        Sends zip to the real storage and deletes it locally.
        :param removeEmpty: If true and zip is empty, delete zip without sending
        :param isSendPut: If true, use put instead of post
        :param isMultipart: If true, upload using form multipart, else use urlencode
        :param multipartFieldName: If isMultipart, name of the form field to which send the file.
        :return:
        """

        super().close()
        if removeEmpty and self.is_zip_empty():
            os.remove(self.zipPath)
        else:
            f = open(self.zipPath, 'rb')
            try:
                resp = None
                if (isMultipart):
                    resp = requests.post(self.postUrl,
                                         files={multipartFieldName: (self.zipFileName, f)})
                else:
                    if(isSendPut):
                        resp = requests.put(self.postUrl, data=f, headers={'Content-Type': 'application/octet-stream'})
                    else:
                        resp = requests.post(self.postUrl, data=f, headers={'Content-Type': 'application/octet-stream'})
                if not (resp.status_code == 200 or resp.status_code == 201 or resp.status_code == 204):
                    raise UploadUrlFailException(resp.content)
            except requests.exceptions.MissingSchema as e:
                raise UploadUrlFailException(e)
            # remove the file
            f.close()
            self.clear()

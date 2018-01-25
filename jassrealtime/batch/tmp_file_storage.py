import os, errno, zipfile, json
from uuid import uuid1
from enum import Enum, unique
from ..core.esutils import get_settings
from ..core.settings_utils import get_jass_tmp_dir
from ..core.utils import utf8_json_dump

UNCOMPRESSED_FLUSH_FILE_SIZE = 1000000000


class TmpFileStorage:
    """
    This class is used to abstract a file storage system used by Jass,
    """

    def __init__(self, zipFileName: str = None):
        """
        Createa a file in temporary directory.
        """
        sett = get_settings()
        self.tmpDirPath = get_jass_tmp_dir()
        self.mkdir_p(self.tmpDirPath)
        self.zipFileName = zipFileName
        if (not zipFileName):
            self.zipFileName = str(uuid1()) + ".zip"
        self.zf = None

    def create_zip_file(self):
        """
        Creates a zip file in a temporary folder. If a zip file with the same name exists,
         it will attempt to overwrite it. Please note that
        """
        self.close()

        self.zipPath = os.path.join(self.tmpDirPath, self.zipFileName)
        zf = zipfile.ZipFile(self.zipPath, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        zf.close()
        self.fileSizeNotSaved = 0

    def add_json_file(self, jsonData: dict, workFileName: str):
        """
        Creates a new file and adds it to zip.

        :param workFilePath:
        :return:
        """
        self._open_zip()
        data = utf8_json_dump(jsonData)
        self.zf.writestr(workFileName, data)
        self._flush(len(data))

    def add_utf8_file(self, data: str, workFileName: str):
        """
        Adds a utf8 file to zip
        :param data: data to save as file
        :param workFileName: name of the file to add to zip. Needs to be alphanumeric with -_.
        :return:
        """
        self._open_zip()
        self.zf.writestr(workFileName, data)
        self._flush(len(data))

    def _open_zip(self):
        if not self.zf:
            self.zf = zipfile.ZipFile(self.zipPath, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True)

    def _flush(self, fileSize=-1):
        if fileSize != -1:
            self.fileSizeNotSaved += fileSize
            if self.fileSizeNotSaved > UNCOMPRESSED_FLUSH_FILE_SIZE:
                self.fileSizeNotSaved = 0
                self.close()
                self._open_zip()
        else:
            self.close()
            self._open_zip()

    def close(self):
        if self.zf:
            self.zf.close()
        self.zf = None

    def is_zip_empty(self):
        zf = zipfile.ZipFile(self.zipPath)
        if not zf.namelist():
            return True
        return False

    def clear(self):
        """
        Removes the
        :return:
        """
        if os.path.isfile(self.zipPath):
            os.remove(self.zipPath)

    def mkdir_p(self, path):
        """
        Makes directory recursively
        http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
        :return:
        """
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

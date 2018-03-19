import importlib.util
import os

_SETT = None
_SETT_PATH = None

from jassrealtime.core.settings import _SETTINGS, JASS_ENV, JASS_MANAGE_ENV, JASS_REBUILD_ENV, \
    FILE_STORAGE_DATA_URL, JASS_TMP_DIR, JASS_ALLOW_CORS, JASS_LOG_LEVEL, JASS_NB_CORES, \
    NUMBER_OF_SHARDS, NUMBER_OF_REPLICAS, NB_DOCUMENTS_PER_SCAN_SCROLL, JASS_EXPOSE_SWAGGER
from jassrealtime.core.language_manager import LanguageManager


def get_settings():
    """
    Returns a settings object
    :return settings nested dictionnary
    """
    global _SETT
    if _SETT is not None:
        return _SETT
    #
    # if _SETT_PATH:
    #     spec = importlib.util.spec_from_file_location("settings", _SETT_PATH)
    #     settings_module = importlib.util.module_from_spec(spec)
    #     spec.loader.exec_module(settings_module)
    #     _SETT = settings_module._SETTINGS
    # if "JIAS_SETTINGS_PATH" in os.environ:
    #     spec = importlib.util.spec_from_file_location("settings", os.environ.get("JIAS_SETTINGS_PATH", None))
    #     settings_module = importlib.util.module_from_spec(spec)
    #     spec.loader.exec_module(settings_module)
    #     _SETT = settings_module._SETTINGS
    # else:
    #     from jassrealtime.core.settings import _SETTINGS
    #     _SETT = _SETTINGS
    #
    _SETT = _SETTINGS
    return _SETT


def get_env_id():
    return JASS_ENV


def can_manage_env():
    return JASS_MANAGE_ENV


def can_rebuild_env():
    return JASS_REBUILD_ENV


def get_file_storage_data_url():
    return FILE_STORAGE_DATA_URL


def get_jass_tmp_dir():
    return JASS_TMP_DIR


def get_jass_allow_cors():
    return JASS_ALLOW_CORS


def reload_setting():
    """
    Clears settings cache.
    Should not be used normally
    """
    global _SETT
    _SETT = None


def get_language_manager():
    sett = get_settings()
    return LanguageManager(sett['LANGUAGE']['LANGUAGE_TO_ES_CONVERSION'])


def set_setting_path(path=None):
    _SETT_PATH = path

    # TODO test remote module


def get_log_level():
    return JASS_LOG_LEVEL


def get_nb_cores():
    if JASS_NB_CORES:
        return int(JASS_NB_CORES)

    return 1


def get_scan_scroll_duration():
    SETT = get_settings()
    return SETT["ELASTIC_SEARCH"]["scan_scroll_duration"]


def get_number_of_shards():
    return NUMBER_OF_SHARDS


def get_number_of_replicas():
    return NUMBER_OF_REPLICAS


def get_nb_documents_per_scan_scroll():
    return NB_DOCUMENTS_PER_SCAN_SCROLL


def get_expose_swagger():
    return (JASS_EXPOSE_SWAGGER == "True")
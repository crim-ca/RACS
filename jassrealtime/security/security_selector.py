from typing import Dict
from .base_authorization import BaseAuthorization

def get_autorisation(envId:str,userToken,tokenType) -> BaseAuthorization:
    """
    Returns an authorisation oibject necessary to acces any jass functionality

    :param envId:   Id of the env which user want to access
    :param userToken:  Object containing token data
    :param tokenType:  Describes the type of token used.
    :return:           An authorisation object. All authorisation objects subclass BaseAuthorisation.
    """

    #TODO
    return BaseAuthorization.create_authorization(envId,userToken,tokenType)
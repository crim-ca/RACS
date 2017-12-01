from typing import Dict

def get_token_info(jwtToken) -> Dict:
    """
    TODO: create implementation for JWT token

    :param jwtToken: jwt token containing permissions
    :return: dictionary: {"username": ...,"env"  }. Username for whom token was issues.
        env on which to execute the action. Optional if performaing env creation/deletion.
    """

    return {"username":"test","env":"test"}

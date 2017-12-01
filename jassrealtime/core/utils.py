import json
from uuid import uuid1


def utf8_json_dump(data: dict):
    return json.dumps(data, ensure_ascii=False).encode('utf8')


# Why we use UUID1 http://stackoverflow.com/questions/1785503/when-should-i-use-uuid-uuid1-vs-uuid-uuid4-in-python
def gen_uuid() -> str:
    return str(uuid1())

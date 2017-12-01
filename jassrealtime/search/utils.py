from typing import List
from ..document.interval import Interval

def add_offset_to_query(esSearchData: dict, offsets: List[Interval]):
    """
    modifies esSearchData to include nested object query

    :param esSearchData:  Dictionary containing the es query and parameters
    :param offsets:       Offsets to search. Currently supports only 1 offset.
    """
    if len(offsets) > 1:
        raise NotImplemented()
    firstOffset = offsets[0]
    begin = firstOffset.begin
    if (firstOffset.openBegin):
        begin = begin + 1

    end = firstOffset.end
    if (firstOffset.openEnd):
        end = end - 1

    if firstOffset.isFullyInclusif:
        raise NotImplemented()
    else:
        nestedQuery = {}
        nestedQuery["nested"] = {}
        nestedQuery["nested"]["path"] = "offsets"
        nestedQuery["nested"]["score_mode"] = "none"
        nestedQuery["nested"]["query"] = {}
        nestedQuery["nested"]["query"]["bool"] = {}
        nestedQuery["nested"]["query"]["bool"]["should"] = [
            {"bool": {"should": [
                {"range": {"offsets.begin": {"gte": begin, "lte": end}}},
                {"range": {"offsets.end": {"gte": begin, "lte": end}}}
            ]}},
            {"bool": {"must": [
                {"range": {"offsets.begin": {"lte": begin}}},
                {"range": {"offsets.end": {"gte": end}}}
            ]}}
        ]
        if "match_all" in esSearchData["query"]:
            esSearchData["query"]["bool"] = {"filter" : [nestedQuery]}
            del esSearchData["query"]["match_all"]
        else:
            if not "filter" in esSearchData["query"]["bool"]:
                esSearchData["query"]["bool"]["filter"] = []
            esSearchData["query"]["bool"]["filter"].append(nestedQuery)

def replaceFieldNames(data:List,originalName:str,newName:str):
    """
    Replace
    :param data:
    :param sourceField:
    :param destinationField:
    :return:
    """
    for item in data:
        item[newName] = item.pop(originalName)

def deleteField(data:list,fieldName:str):
    for item in data:
        del item[fieldName]
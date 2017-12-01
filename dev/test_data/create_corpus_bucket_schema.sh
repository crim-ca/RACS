#!/usr/bin/env bash

# creates bucket,corpus and schemas

jassloc="localhost:8888"
corpus="corptest"
bucket1="bucket1"
bucket2="bucket2"
docschema="sentences"
tokeschema="token"
tokenwithlemmaschema="tokenwithlemma"

# create corpus
curl -H "Content-Type: application/json" -X POST -d '{"id": "corptest","title":"Test Corpus","platformId":"test","languages":["fr_FR","en_EN"],"description":"Minimalistic corpus for testing jass functionnality","reference":"http://doesnotexist.com","source":"Testing Source","version":"1.0"}' "http://$jassloc/corpora"
sleep 1
curl -H "Content-Type: application/json" -X POST -d '{"id": "bucket1","name":"Bucket with document annotations"}' "http://$jassloc/corpora/$corpus/buckets"
curl -H "Content-Type: application/json" -X POST -d '{"id": "bucket2","name":"Bucket with surface annotations only"}' "http://$jassloc/corpora/$corpus/buckets"
sleep 1
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/schemas" -d @- << EOF
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "targetType": "document",
    "schemaType": "sentence",
    "type": "object",
    "required": [
        "schemaType",
        "_corpusID",
        "_documentID",
        "sentence"
    ],
    "properties": {
        "schemaType": {
            "type": "string",
            "description": "Schema type",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_documentID": {
            "type": "string",
            "description": "Internal document GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_corpusID": {
            "type": "string",
            "description": "Internal Corpus GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "sentence": {
            "type": "string",
            "description": "Sentence in a document",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        }
    }
}
EOF

curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/schemas" -d @- << EOF
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "targetType": "document_surface1d",
    "schemaType": "token",
    "type": "object",
    "required": [
        "schemaType",
        "_corpusID",
        "_documentID",
        "sentence"
    ],
    "properties": {
        "schemaType": {
            "type": "string",
            "description": "Schema type",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_documentID": {
            "type": "string",
            "description": "Internal document GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_corpusID": {
            "type": "string",
            "description": "Internal Corpus GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "word": {
            "type": "string",
            "description": "Word in a document",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "length": {
            "type": "integer",
            "description": "Length of a word",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "category": {
            "type": "string",
            "description": "category of the word",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "offsets": {
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true,
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "properties": {
                    "begin": {"type": "integer", "minimum": 0},
                    "end": {"type": "integer", "minimum": 0}
                }
            }
        }
    }
}
EOF


curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/schemas" -d @- << EOF
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "targetType": "document_surface1d",
    "schemaType": "tokenwithlemma",
    "type": "object",
    "required": [
        "schemaType",
        "_corpusID",
        "_documentID",
        "sentence"
    ],
    "properties": {
        "schemaType": {
            "type": "string",
            "description": "Schema type",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "_documentID": {
            "type": "string",
            "description": "Internal document GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_corpusID": {
            "type": "string",
            "description": "Internal Corpus GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "word": {
            "type": "string",
            "description": "Word in a document",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "length": {
            "type": "integer",
            "description": "Length of a word",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "lemma": {
            "type": "string",
            "description": "Lemma of a word",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "category": {
            "type": "string",
            "description": "category of the word",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "offsets": {
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true,
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "properties": {
                    "begin": {"type": "integer", "minimum": 0},
                    "end": {"type": "integer", "minimum": 0}
                }
            }
        }
    }
}
EOF

curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/schemas" -d @- << EOF
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "targetType": "document_surface1d",
    "schemaType": "token",
    "type": "object",
    "required": [
        "schemaType",
        "_corpusID",
        "_documentID",
        "sentence"
    ],
    "properties": {
        "schemaType": {
            "type": "string",
            "description": "Schema type",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_documentID": {
            "type": "string",
            "description": "Internal document GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_corpusID": {
            "type": "string",
            "description": "Internal Corpus GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "word": {
            "type": "string",
            "description": "Word in a document",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "length": {
            "type": "integer",
            "description": "Length of a word",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "category": {
            "type": "string",
            "description": "category of the word",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "offsets": {
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true,
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "properties": {
                    "begin": {"type": "integer", "minimum": 0},
                    "end": {"type": "integer", "minimum": 0}
                }
            }
        }
    }
}
EOF


curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/schemas" -d @- << EOF
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "targetType": "document_surface1d",
    "schemaType": "tokenwithlemma",
    "type": "object",
    "required": [
        "schemaType",
        "_corpusID",
        "_documentID",
        "sentence"
    ],
    "properties": {
        "schemaType": {
            "type": "string",
            "description": "Schema type",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "_documentID": {
            "type": "string",
            "description": "Internal document GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "_corpusID": {
            "type": "string",
            "description": "Internal Corpus GUID",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "word": {
            "type": "string",
            "description": "Word in a document",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "length": {
            "type": "integer",
            "description": "Length of a word",
            "searchable": true,
            "searchModes": ["noop"],
            "locked": true
        },
        "lemma": {
            "type": "string",
            "description": "Lemma of a word",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "category": {
            "type": "string",
            "description": "category of the word",
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true
        },
        "offsets": {
            "searchable": true,
            "searchModes": ["basic"],
            "locked": true,
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "properties": {
                    "begin": {"type": "integer", "minimum": 0},
                    "end": {"type": "integer", "minimum": 0}
                }
            }
        }
    }
}
EOF

sleep 1



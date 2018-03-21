#!/usr/bin/env bash

jassloc="localhost:8888"
corpus="corptest"
bucket1="bucket1"
bucket2="bucket2"
docschema="sentence"
tokeschema="token"
tokenwithlemmaschema="tokewithlemma"

#  sentences
# bucket 1
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"sentence", "sentence": "Les algorithmes de colonies de fourmis sont des algorithmes inspirés du comportement des fourmis."}
EOF

curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"sentence", "sentence": "Le café liégeois doit son appellation à la résistance de l’armée belge lors de la bataille des forts de Liège d’août 1914."}
EOF

# bucket1
# token
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "Les","offsets": [{"begin":0, "end":3 }], "length": 3, "category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "algorithmes","offsets": [{"begin":4, "end":15 }],"length": 11, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "de", "offsets": [{"begin":28, "end":30 },{"begin":16, "end":18 }], "length": 2,"category": "PRP"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "colonies", "offsets": [{"begin":19, "end":27 }], "length": 8,"category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "fourmis","offsets": [{"begin":31, "end":38 }], "length": 7, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "sont", "offsets": [{"begin":39, "end":43 }], "length": 4, "category": "VER:pres"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "des","offsets": [{"begin":44, "end":47 }], "length": 3, "category": "PRP:det"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "algorithmes","offsets": [{"begin":48, "end":59 }], "length": 11, "category": "NOM"}
EOF

# tokewithlemma
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "Le","offsets": [{"begin":98, "end":100 }],"length": 2,"lemma": "le","category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "café","offsets": [{"begin":101, "end":105 }], "length": 4,"lemma": "café","category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "liégeois","offsets": [{"begin":106, "end": 114}], "length": 8,"lemma": "liégeois","category": "ADJ"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "doit","offsets": [{"begin":115, "end": 119}], "length": 4,"lemma": "devoir","category": "VER:pres"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "son", "offsets": [{"begin":120, "end":123 }], "length": 3,"lemma": "son", "category": "DET:POS"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "appellation", "offsets": [{"begin":124, "end":135 }], "length": 11, "lemma": "appellation", "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "à", "offsets": [{"begin":136, "end": 137}], "length": 1,"lemma": "à", "category": "PRP"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "la","offsets": [{"begin":138, "end":140 }], "length": 2,"lemma": "le","category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "résistance","offsets": [{"begin":141, "end":151 }], "length": 10, "lemma": "résistance","category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "de","offsets": [{"begin":152, "end":154 }], "length": 2,"lemma": "de","category": "PRP"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "l", "offsets": [{"begin":155, "end": 156}], "length": 1,"lemma": null,"category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "armée", "offsets": [{"begin":157, "end":162 }], "length": 5,"lemma": "armer", "category": "VER:pper"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "belge", "offsets": [{"begin":163, "end":168 }], "length": 5,"lemma": "belge", "category": "ADJ"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "lors","offsets": [{"begin":169, "end": 173}], "length": 4,"lemma": "lors","category": "ADV"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "de","offsets": [{"begin":174, "end":176 }], "length": 2,"lemma": "de","category": "PRP"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "la","offsets": [{"begin":177, "end":179 }], "length": 2,"lemma": "le","category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "bataille","offsets": [{"begin":180, "end":188 }], "length": 8,"lemma": "bataille","category": "NOM"}
EOF

# bucket2
# token
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "algorithmes","offsets": [{"begin":48, "end":59 }], "length": 11, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "inspirés", "offsets": [{"begin":60, "end":68 }], "length": 8, "category": "VER:pper"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "du", "offsets": [{"begin":69, "end":71 }], "length": 2, "category": "PRP:det"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "comportement", "offsets": [{"begin":72, "end":84 }], "length": 12, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "des","offsets": [{"begin":85, "end":88 }], "length": 3, "category": "PRP:det"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": "fourmis","offsets": [{"begin":89, "end":96 }], "length": 7, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"token", "word": ".","offsets": [{"begin":96, "end":97 }], "length": 1, "category": "SENT"}
EOF

# tokewithlemma
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "armée", "offsets": [{"begin":157, "end":162 }], "length": 5,"lemma": "armer", "category": "VER:pper"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "belge", "offsets": [{"begin":163, "end":168 }], "length": 5,"lemma": "belge", "category": "ADJ"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "lors","offsets": [{"begin":169, "end": 173}], "length": 4,"lemma": "lors","category": "ADV"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "de","offsets": [{"begin":174, "end":176 }], "length": 2,"lemma": "de","category": "PRP"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "la","offsets": [{"begin":177, "end":179 }], "length": 2,"lemma": "le","category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "bataille","offsets": [{"begin":180, "end":188 }], "length": 8,"lemma": "bataille","category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "des", "offsets": [{"begin":189, "end":192 }], "length": 3,"lemma": "du","category": "PRP:det"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "forts", "offsets": [{"begin":193, "end":198 }], "length": 5,"lemma": "fort","category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "de","offsets": [{"begin":199, "end":201 }], "length": 2,"lemma": "de","category": "PRP"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "Liège", "offsets": [{"begin":202, "end":207 }], "length": 5,"lemma": "Liège", "category": "NAM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "d", "offsets": [{"begin":208, "end":209 }], "length": 1,"lemma": null,"category": "VER:futu"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "août","offsets": [{"begin":210, "end":214 }], "length": 4,"lemma": "août","category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "1914","offsets": [{"begin":215, "end":219 }], "length": 4,"lemma": "@card@","category": "NUM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc1","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": ".", "offsets": [{"begin":219, "end":220 }], "length": 1,"lemma": ".", "category": "SENT"}
EOF


# doc 2
# bucket1
# token
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"token", "word": "sont", "offsets": [{"begin":39, "end":43 }], "length": 4, "category": "VER:pres"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"token", "word": "des","offsets": [{"begin":44, "end":47 }], "length": 3, "category": "PRP:det"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"token", "word": "algorithmes","offsets": [{"begin":48, "end":59 }], "length": 11, "category": "NOM"}
EOF

# tokewithlemma
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "de","offsets": [{"begin":174, "end":176 }], "length": 2,"lemma": "de","category": "PRP"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "la","offsets": [{"begin":177, "end":179 }], "length": 2,"lemma": "le","category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "bataille","offsets": [{"begin":180, "end":188 }], "length": 8,"lemma": "bataille","category": "NOM"}
EOF

# bucket2
# token
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"token", "word": "des","offsets": [{"begin":85, "end":88 }], "length": 3, "category": "PRP:det"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"token", "word": "fourmis","offsets": [{"begin":89, "end":96 }], "length": 7, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"token", "word": ".","offsets": [{"begin":96, "end":97 }], "length": 1, "category": "SENT"}
EOF

# tokewithlemma
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "août","offsets": [{"begin":210, "end":214 }], "length": 4,"lemma": "août","category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "1914","offsets": [{"begin":215, "end":219 }], "length": 4,"lemma": "@card@","category": "NUM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc2","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": ".", "offsets": [{"begin":219, "end":220 }], "length": 1,"lemma": ".", "category": "SENT"}
EOF

# doc 3
# bucket1
# token
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"token", "word": "Les","offsets": [{"begin":0, "end":3 }], "length": 3, "category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"token", "word": "algorithmes","offsets": [{"begin":4, "end":15 }],"length": 11, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"token", "word": "de", "offsets": [{"begin":28, "end":30 },{"begin":16, "end":18 }], "length": 2,"category": "PRP"}
EOF

# tokewithlemma
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "Le","offsets": [{"begin":98, "end":100 }],"length": 2,"lemma": "le","category": "DET:ART"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "café","offsets": [{"begin":101, "end":105 }], "length": 4,"lemma": "café","category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket1/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "liégeois","offsets": [{"begin":106, "end": 114}], "length": 8,"lemma": "liégeois","category": "ADJ"}
EOF

# bucket2
# token
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"token", "word": "algorithmes","offsets": [{"begin":48, "end":59 }], "length": 11, "category": "NOM"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"token", "word": "inspirés", "offsets": [{"begin":60, "end":68 }], "length": 8, "category": "VER:pper"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"token", "word": "du", "offsets": [{"begin":69, "end":71 }], "length": 2, "category": "PRP:det"}
EOF

# tokewithlemma
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "armée", "offsets": [{"begin":157, "end":162 }], "length": 5,"lemma": "armer", "category": "VER:pper"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "belge", "offsets": [{"begin":163, "end":168 }], "length": 5,"lemma": "belge", "category": "ADJ"}
EOF
curl -H "Content-Type: application/json" -X POST "http://$jassloc/corpora/$corpus/buckets/$bucket2/annotations" -d @- << EOF
{"_documentID":"doc3","_corpusID":"corptest","schemaType":"tokenwithlemma", "word": "lors","offsets": [{"begin":169, "end": 173}], "length": 4,"lemma": "lors","category": "ADV"}
EOF
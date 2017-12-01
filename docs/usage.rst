=====
Usage
=====

Overview
========
The two main uses of the RACS is to store document texts into corpora and to store annotations associated a corpora or to a document.

Each corpus can be associated with either French, English or both. Behind the scene, an Elasticsearch index is created for each language.

Annotations are stored as JSON and follow certain restrictions on top of a JSON Schema.

Examples of the kind of acceptable JSON schemas are found on this annotation and schema validator: https://github.com/crim-ca/json-schema-validation-service/tree/master/data/pacteSchemas

Warning: this project might not be totally in sync with the RACS.

Before storing a corpus annotation, a schema of its corresponding schemaType property must be associated to a bucket.

Buckets are created for a corpus in particular. No more than one schema of the same schemaType can be associated to the same bucket.

PS: RACS do not validate that the documentId of an annotation references a document in it's corpus. It is entirely possible to use the RACS to only store documents or to only store annotations.

API
---

For details on the API, you can consult the generated Swagger documentation (see installation.)

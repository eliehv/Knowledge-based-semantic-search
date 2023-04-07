# Knowledge-based-semantic-search
design and implement a Natural Language Processing Pipeline by defining microservices

In this pipeline _Jina ai_ is used as ML platform and _DocArray_ as a proper data structure for nested, unstructured, multimodal data in transit.
The activities in the pipeline includes:
* Data preparation
  * crawling the source of knowledge to extract text (unstructured data)
  * partition them accordingly to the chunks
  * store it in the form of key-value Documents
  * Load the documents to the Amazon S3 (storage)
* Ingest data from S3 to the Jina executor in form of DocArray
* Index 
  * Encoder - uses Huggingface sentence transformers to create embedding for chunks of text
  * Indexer - uses a (SQLite) database for indexing Document
* Search 
  *  leverages DocumentArray's match function and searches the k nearest neighbors for the query Document based on their embedding field 
  *  utilizes ranking to find the best match as final result
  

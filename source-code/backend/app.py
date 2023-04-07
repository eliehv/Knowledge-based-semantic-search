from docarray import DocumentArray, Document
from jina import Flow, DocumentArray, Executor, requests
from config import PORT #PORT = 23457
import click

import logging
import boto3
import botocore
from botocore.exceptions import ClientError
import os
import glob
import json
import configparser
import csv
import re
import pickle

def readConfig():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    return config['S3']

config = readConfig()

#********************************************************

# ***************** Executor to filter out the Documents without embedding *********
#EMB_DIM = 512

class EmbeddingChecker(Executor):
    @requests
    def check(self, docs, **kwargs):
        filtered_docs = DocumentArray()
        for doc in docs:
            if doc.embedding is None:
                continue
            #if doc.embedding.shape[0] != EMB_DIM:
            #    continue
            filtered_docs.append(doc)
        return filtered_docs
    
flow = (
    Flow(protocol="http", port=PORT)
    .add(
        name="encoder",
        uses="jinahub://TransformerTorchEncoder",
     
        uses_with={
            "pretrained_model_name_or_path": "sentence-transformers/paraphrase-mpnet-base-v2"
        #uses_with={'pretrained_model_name_or_path': 'bert-base-uncased'#, 'match_args': {'limit': 10}
            ,'traversal_right': '@r'
            }
        ,
        install_requirements=True,
    )
    .add(name = "Filterout_docs_without_embeddings", uses = EmbeddingChecker)
    .add(name = "Indexer",uses="jinahub://SimpleIndexer", needs='Filterout_docs_without_embeddings',  install_requirements=True)
    .needs_all()
)
#flow.plot()


def index():
    
    s3 = boto3.resource('s3')
    bucket_name = config['bucket_name']
    bucket = s3.Bucket(bucket_name)
    print(bucket_name)
    docs = DocumentArray()
    for obj in bucket.objects.all():
        key = obj.key
        body = obj.get()['Body'].read()
        if key.endswith('.json') :
            data = body.decode('utf-8') # Decode using the utf-8 encoding
            jdata = json.loads(data)
            body = jdata['body']
            
            #print(type(body))
            #print(body)
            doc = Document(text = jdata['title'], uri = jdata['pageLink'], tags = jdata)
            for item in body:
                text = ""
                for k,v in item.items():
                    text = "\n".join([text,v])
                #print(text)
                doc.chunks.append(Document(text = text, uri = jdata['pageLink']))
        docs.append(doc)
    docs.summary()
    #qa_docs = 
    with flow:
        #docs = flow.index(docs, on_done = store_embeddings ,show_progress=True)
        index_output = flow.index(docs,show_progress=True)
        print(type(index_output))
        print(index_output)
        #docs.summary   
#*******************************************************

def search():
    with flow:
        flow.block()

    
@click.command()
@click.option(
    "--task",
    "-t",
    type=click.Choice(["index", "search"], case_sensitive=False),
)

def main(task: str):
    if task == "index":
        index()
    elif task == "search":
        response = search()
        print(type(response))
        #with open("response-json.json",'w')as outfile:
            #json.dump(response,outfile)
        with open("response-pkl.pickle",'wb')as outfile:
            pickle.dump(response,outfile)
    else:
        print("Please add '-t index' or '-t search' to your command")


if __name__ == "__main__":
    main()

"""
    The main process in this script includes:
        Extract data and put it in a specific formt
        Create S3 bucket
        load data in Amazon S3
"""
import logging
import boto3
import botocore
from botocore.exceptions import ClientError
import os
import glob
import configparser
import re
import json
import requests
import unidecode
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
import pprint
import time

#**************** Read Config *************
def readConfig():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    return config['Data-Metadata']

config = readConfig()

#**************** Create folder to store extracted data locally ********
def create_folder(folder_name):
    current_directory = os.getcwd()
    #folder_name = config['folder_name']
    final_directory = os.path.join(current_directory, r'{}'.format(folder_name))
    if not os.path.exists(final_directory):
       os.makedirs(final_directory)
    print('directory created to store the data files : {}'.format(final_directory))
    
#***************** Extract Data **********************
"""Extract nested values from a JSON tree.
    ******** Extract name and email address of author **********
"""
def json_extract_AuthorInfo(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []
    def extract(obj, arr,key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                arr.append(item)
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values

def _getLinks(_header):
    children = _header.next_sibling.findChildren('a', recursive=True)
    links = [child.get('href') for child in children]
    return(links)


def _get_text(_header, headers):
    text = []
    while _header.next_sibling not in headers and _header.next_sibling:
        text.append(unidecode.unidecode(_header.next_sibling.text))
        _header = _header.next_sibling
    text = ' '.join(text)
    return(text)
"""
    store extrcated data and metadata files separately 
"""
def store_data_metadata_filesLocally(all_content, folder_name):
    
    for i in range(len(all_content)):
        data_dict = all_content[i]
        title = data_dict['title']
        title = "".join(c for c in title if c.isalpha() or c == ' ')
        
        data_file_name = f'{folder_name}/{title}.text'
        metadata_file_name = f'{folder_name}/{title}.text.metadata.json'
        
        with open(data_file_name, 'w') as f:
            file_content = data_dict['body']
            for d in file_content:
                for k,v in d.items():
                    if(k != 'header' and k !='text'):
                        f.write(k)
                    f.write('\n')
                    f.write(v)

        metadata_json_file = {}
        for key,val in data_dict.items():
            
            if key == 'author':
                metadata_json_file.update({'Author':val})
                
            if key == 'createdDate':
                metadata_json_file.update({'CreatedDate':val})
                
            if key == 'pageLink':
                metadata_json_file.update({'pageLink':val})
                
            if key == 'title':
                metadata_json_file.update({'title':val})
                
                
            # there was an error in syncing the data source due to _document_body attribute. 
            #I think the reason is type of stored data in this field, we stored dictionary (json file) 
            #while it is supposed to be string
            
            if key == 'body':
                body_text = []
                for body_item in val:
                    for body_k,body_v in body_item.items():
                        body_text.append(body_v)
                temp = "".join(body_text)
                metadata_json_file.update({'Body_content':temp})
                
            if key == 'type':
                metadata_json_file.update({'FileType':val})
                
            # else:            
        metadata_json_file.update({'_category':'EH'})
                
                
        metadata_json_file_all = {}
        metadata_json_file_all.update({'DocumentId':data_dict['id']})
        metadata_json_file_all.update({'Attributes':metadata_json_file})
        metadata_json_file_all.update({'Title':data_dict['title']})
        metadata_json_file_all.update({'ContentType':'PLAIN_TEXT'})
        
        with open(metadata_file_name, 'w', encoding = 'utf-8') as mf:
            #print(metadata_file_name)          
            mf.write(json.dumps(metadata_json_file_all,ensure_ascii=False))
            #json.dump(metadata_json_file_all, mf, ensure_ascii=False)
                       
""" 

    extract body text besides some metadata like author, uri, creation date, type, title from Employee Handbook
                                                                                                                        
""" 
def extract_data_metadata_EH(config):
    
    folder_name = config['folder_name']
    token =  config['token']
    limit =  1000  
    url = 'https://<Your url>/rest/api/content/?expand=body,parent,history,metadata.properties&limit={}'.format(limit)


    r = requests.get(url, auth=('<Your usr>',token))
    results = r.json()['results']
    titles = [x['title'] for x in results]

    print('number of Extracted pages: ' + str(len(results)))
    
    all_content = []
    for i in range(len(results)):
        content = {}
        for key,val in results[i].items():
            if key == 'id':
                content.update({'id':val})
            if key == 'type':
                content.update({'type':val})
            if key == 'title':
                content.update({'title':val})
            # name and email of author should be extarcted of a nested dict in history key
            if key == 'history':
                content.update({'createdDate':json_extract_AuthorInfo(results[0],'createdDate')[0]})
                #content.update({'author':{'Email':json_extract_AuthorInfo(results[0],'email')[0],
                                #'Name':json_extract_AuthorInfo(results[0],'publicName')[0]}})
                content.update({'author':json_extract_AuthorInfo(results[0],'email')[0]}) 


            # extract body of the document content and page link
            webui = results[i]['_links']['webui']
            textURL = f'https://<Your url>{webui}'
            r2 = requests.get(textURL,auth=('<Your usr>',token))
            soup=BeautifulSoup(r2.text,'html.parser')
            # look for pattern containing h1 to h6 in html (level 1 to 6 of headings)
            headers = soup.find_all(re.compile('^h[1-6]$'))

            txt = []#{}#''
            for h in headers:
                if h.next_sibling:
                    text = _get_text(h, headers)
                    txt.append({'header':h.text,
                                       'text':text})
                    refLinks = _getLinks(h)
            content.update({'pageLink': textURL})
            content.update({'body':txt})
            content.update({'pageLink':textURL})
        all_content.append(content)
    print("The number of Items Extarcted : {}".format(len(all_content)) )
    # call the method to store data and metadata locally
    store_data_metadata_filesLocally(all_content,folder_name)
    return all_content
        
"""
    ************  Create s3 bucket and store documents in it ***************
"""
def create_Bucket(config):

    bucket_name = config['bucket_name']
    aws_access_key_id= config['aws_access_key_id']
    aws_secret_access_key=config['aws_secret_access_key']
    region = config['region']
    bucket_exists = False
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    s3_client = boto3.client('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        if bucket["Name"] == bucket_name:
            print(f'There already exists a bucket called {bucket["Name"]}')
            bucket_exists = True
            break
        

    # Create bucket
    if not bucket_exists:
        try:
            if region is None:
                #s3_client = boto3.client('s3',aws_access_key_id, aws_secret_access_key)
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
    return True


def upload_File(file_name, bucket, config, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    aws_access_key_id = config['aws_access_key_id']
    aws_secret_access_key = config['aws_secret_access_key']
    
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',aws_access_key_id = aws_access_key_id, aws_secret_access_key= aws_secret_access_key)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def put_data_to_S3(config):
    folder_name = config['folder_name']
    #print(folder_name)
    bucket_name = config['bucket_name']
    for filepath in glob.glob(os.path.join(folder_name, '*.*')):#glob.glob(os.path.join('data_metadata', '*.text')):
        #print(filepath)
        print('****** filePath : '+filepath)
        
        object_name = filepath.replace(' ', '-')
        upload_File(filepath, bucket_name, config, object_name)



#     main()
def main():
    # Main Method (Run the pipeline)
    config = readConfig()
    
    #********** Extract data and metadata then store them locally and in s3
    folder_name = config['folder_name']
    create_folder(folder_name)
    all_content = extract_data_metadata_EH(config)
    
    create_Bucket(config)
    put_data_to_S3(config)
    
    
    
main()

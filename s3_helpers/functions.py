import os, boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
from re import sub
import tarfile


def get_s3_client(access = None,
                  secret = None,
                  bucket = 'ds-cloud-cso',
                  use_creds = False):
    
    """
    
    Initialize s3 connection client
    
    """
    
    if use_creds:
        s3 = boto3.client(service_name = 's3', 
                          aws_access_key_id = access, 
                          aws_secret_access_key = secret)
    else:
        s3 = boto3.client(service_name = 's3', 
                          config=Config(signature_version=UNSIGNED))
    
    return s3, bucket
    
def s3_CSVtoDF(file_name, use_creds = True, **kwargs):
    
    """
    
    Stream a data file from S3 into a dataframe
    
    All **kwargs are passed to pandas.read_csv() and must
    therefore be valid keyword arguments of that function
    
    """
    
    s3, bucket = get_s3_client(use_creds = use_creds)
    obj = s3.get_object(Bucket=bucket, Key=file_name)
    
    return pd.read_csv(obj['Body'], **kwargs)


def push_file_to_s3(path,key=None, use_creds = True):
    
    """Take in a path and push to S3"""

    if key == None:
        print ("Can't push to S3 without a key. Please specify a key.")
        return

    key = key.replace(' ','-')

    s3, bucket = get_s3_client(use_creds = use_creds)
    
    s3.upload_file(path, bucket, key)
    print ("Sent file %s to S3 with key '%s'"%(path,key))
        
        
def pull_file_from_s3(key, path, use_creds = True):
    
    local_dir = '/'.join(path.split('/')[:-1])
    if not os.path.isdir(local_dir) and local_dir != '':
        print ("Local directory %s doesn't exist"%(local_dir))
        return
    
    s3, bucket = get_s3_client(use_creds = use_creds)
    s3.download_file(bucket, key, path)
    

    print ("Grabbed %s from S3. Local file %s is now available."%(key,path))
            
def s3_fetch_module(s3_path, file_name, use_creds = True):
    
    """

    Fetch a module in a tar.gz file from s3
    
    """
    
    s3, bucket = get_s3_client(use_creds = use_creds)
    print('Fetching ' + s3_path + file_name)
    s3.download_file(Bucket=bucket, Key=s3_path + file_name, Filename=file_name)

                
    dir_name = sub('.tar.gz$', '', file_name)
    
    contains_hyphen = False
    if '-' in dir_name:
        contains_hyphen = True
        print("Module name contains invalid '-' hyphens.  Replacing with '_' underscores")
        dir_name = dir_name.replace('-','_')
    
    
    try:
        shutil.rmtree('./' + dir_name)
        print('Removing old module ' + dir_name)
    except:
        pass
    
    print('Extracting ' + file_name + ' into ' + dir_name)
    archive = tarfile.open(file_name, 'r:gz')
    archive.extractall('./')
    archive.close()
    
    if contains_hyphen:
        os.rename(dir_name.replace('_','-'), dir_name)
        
    try:
        os.remove(file_name)
        print('Removing ' + file_name)
    except:
        pass
    
    if ~os.path.exists(dir_name + '/__init__.py'):
        print('__init__.py not found.  Creating it in ' + dir_name)
        open(dir_name + '/__init__.py','w').close()
        
def s3_ls(path, use_creds = True):
    
    """
    
    List contents of s3 directory specified in 'path'
    
    """
    
    s3, bucket = get_s3_client(use_creds = use_creds)
    
    if path != '' and path[-1] != '/':
        path += '/'
        
    files = []
    directories = []
    
    try:
        for fname in s3.list_objects(Bucket=bucket, Prefix = path)['Contents']:
            
            if '/' not in fname['Key'].replace(path,''):
                files.append(fname['Key'].replace(path,''))
            elif fname['Key'].replace(path,'').split('/')[0] + '/' not in directories:
                directories.append(fname['Key'].replace(path,'').split('/')[0] + '/')
                
    except KeyError:
        return 'Directory Not Found'

    return directories + files

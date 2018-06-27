import json

with open('config.json', 'r') as f:
    config = json.load(f)

def getAwsKey():
    return config['AWS_KEY'], config['AWS_SECRET']

def getFileStorePath():
    return config['CDS_REC_S3']

def getFileCDSStorePath():
    return config['CDL_CDS_S3']

def getModelSavePath():
    return config['CDS_REC_S3_FINAL']

def getModelBackupPath():
    return config['CDS_REC_S3_FINAL_BACKUP']
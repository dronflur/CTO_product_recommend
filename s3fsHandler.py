
import s3fs
from npHandler import saveNp
from configHandler import *
import datetime

CDS_REC_S3_FINAL = getModelSavePath()
CDS_REC_S3_FINAL_BACKUP = getModelBackupPath()

class S3fsHandler:
    fs = None

    def __init__(self, aws_key, aws_secret_key):
        self.fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret_key)
        print('initiate s3fs object')

    def putFile(self, path, save_object):
        if self.fs.exists(path):
            self.fs.rm(path)
        with self.fs.open(path, 'wb') as f:
            saveNp(f, save_object)
        print(path, ' was saved')

    def putFileWithBackup(self, filename, save_object, SavePath = CDS_REC_S3_FINAL, BackupPath = CDS_REC_S3_FINAL_BACKUP):
        sPath = SavePath+filename
        bPath = BackupPath+filename+'_'+datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.putFile(sPath, save_object)
        self.putFile(bPath, save_object)
        
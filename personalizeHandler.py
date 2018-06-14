import pandas as pd
import time
from sparkHandler import saveToTempTable
from sqldata import *

def personalize(alsModel, SavePath):
    start = time.time()
    model_rec = alsModel.model.predictAll(alsModel.joined_rdd).toDF()
    stop = time.time()
    print("done -- predict model " + str(stop-start))

    start = time.time()
    saveToTempTable(DFObject = model_rec, TableName='tbRec')
    stop = time.time()
    print("done -- dump model_rec to temp table " + str(stop-start))
    
    dfProductRec = saveToTempTable(Sql = sql_personalize)
    return dfProductRec
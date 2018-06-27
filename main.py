from sparkHandler import saveToTempTable, loadFile
from alsHandler import runModel
from personalizeHandler import personalize
from s3fsHandler import S3fsHandler
from configHandler import *
from npHandler import saveNp
from sqldata import *
import numpy as np
import time

AWS_KEY, AWS_SECRET = getAwsKey()
CDS_REC_S3 = getFileStorePath()
CDS_REC_S3_FINAL = getModelSavePath()
CDS_REC_S3_FINAL_BACKUP = getModelBackupPath()
CDL_CDS_S3 = getFileCDSStorePath()

s3fsHandler = S3fsHandler(AWS_KEY, AWS_SECRET)

def main():
    bestSeller_var, feq_online_var, feq_offline_var = prepareData()
    bestSeller(bestSeller_var)
    #non_variety, variety = ALSFunction()
    
    start = time.time()
    #CombineAndSave(feq_online_var, feq_offline_var, non_variety, variety)
    stop = time.time()
    print("Final complete --  " + str(stop - start))

def prepareData():
    '''
    # Product Price & Discount
    saveToTempTable(Path = CDS_REC_S3+'CDS_Price.csv', TableName = 'tbProduct_Price')
    # Master Product
    saveToTempTable(Path = CDS_REC_S3+'ProductMaster_Online.csv', TableName = 'tbProduct')
    # Sales 2018
    saveToTempTable(Path = CDS_REC_S3+'CDS_Raw3_2018.csv', TableName = 'tbSales_2018')
    # Sales 2017
    saveToTempTable(Path = CDS_REC_S3+'CDS_Raw2.csv', TableName = 'tbSales_2017')
    '''
    saveToTempTable(Path = CDL_CDS_S3+'cds_dbcdsdata/tborder/*', IsParquet=True, TableName = 'tbOrder')

    saveToTempTable(Path = CDL_CDS_S3+'cds_dbcdsdata/tborderdetail/*', IsParquet=True, TableName = 'tbOrderDetail')

    saveToTempTable(Path = CDL_CDS_S3+'cds_dbcdsdata/tbUser/*', IsParquet=True, TableName = 'tbUser')

    saveToTempTable(Path = CDL_CDS_S3+'cds_dbcdscontent/tbdepartment/*', IsParquet=True, TableName = 'tbDepartment')

    saveToTempTable(Path = CDL_CDS_S3+'cds_dbcdscontent/tbproduct/*', IsParquet=True, TableName = 'tbProduct')

    saveToTempTable(Path = CDL_CDS_S3+'cds_dbcdscontent/tbproductgroup/*', IsParquet=True, TableName = 'tbProductGroup')

    # Sales(Best Seller) -- Test
    #saveToTempTable(Path = CDS_REC_S3+'CDS_Sales_201805_Test.csv', TableName = 'tbSales')

    # Sales(Best Seller)
#    saveToTempTable(Path = CDS_REC_S3+'CDS_Sales_201805.csv', TableName = 'tbSales_bestseller')
    saveToTempTable(Sql = sql_best_seller, TableName = 'tbSales_bestseller')
    # Sales clean
    #saveToTempTable(Sql = sql_sales_clean, TableName = 'tbSales_2017')
    # Merge Sales 2017&2018
    #saveToTempTable(Sql = sql_merge_sales_17_18, TableName = 'tbSales')
    # Sales 2018 (Offline)
    #saveToTempTable(Path = CDS_REC_S3+'CDSSales_2018_Offline.csv', TableName = 'tbCDSSales_2018_Offline')
    # User Master 2018
    #saveToTempTable(Path = CDS_REC_S3+'CDSUser_2018_Update.csv', TableName = 'tbUser_2018')
    # Top10 Product
    saveToTempTable(Sql = sql_top_10_products, TableName = 'tbTop10Product')
    # Top7 All Categories
    saveToTempTable(Sql = sql_top_7_cat_all, TableName = 'tbTop7Cate_All')
    # Top7 All Genders
    saveToTempTable(Sql = sql_top_7_cat_gender, TableName = 'tbTop7Cate_Gender')
    # Top7 All Age
    saveToTempTable(Sql = sql_top_7_cat_age, TableName = 'tbTop7Cate_AgeRange')
    # Top7 All Genders & Age
    saveToTempTable(Sql = sql_top_7_cat_gender_age, TableName = 'tbTop7Cate_GenderAgeRange')
    # Top5 Categories Demo
    bestSeller = saveToTempTable(Sql = sql_top_5_cat_demo)
    # Frequency Online
    #feq_online = saveToTempTable(Sql = sql_feq_online)
    # Frequency Offline
    #feq_offline = saveToTempTable(Sql = sql_feq_offline)
    feq_online = None
    feq_offline = None
    return bestSeller, feq_online, feq_offline

def ALSFunction():
    ### ALS ###
    print("##ALS##")
    var_model = runModel(sql_var_als, 'tbVariety_ALS')
    print("done -- var")
    start = time.time()
    variety = personalize(var_model, SavePath = CDS_REC_S3_FINAL+'Recommend_Variety.csv')
    stop = time.time()
    print("done -- personalize -- var " + str(stop - start))

    non_var_model = runModel(sql_non_var_als, 'tbNonVariety_ALS')
    print("done -- non_var")
    start = time.time()
    non_variety = personalize(non_var_model, SavePath = CDS_REC_S3_FINAL+'Recommend_NonVariety.csv')
    stop = time.time()
    print("done -- personalize -- non_var " + str(stop - start))

    return non_variety, variety

def CombineAndSave(feq_online, feq_offline, non_variety, variety):
    # Recommendation
    print("start -- CombineAndSave")
    dfRecommend = non_variety.union(variety)    
    print("done -- dfRecommend")
    saveToTempTable(DFObject=dfRecommend, TableName='tbRecommend')
    #spark.createDataFrame(dfRecommend).registerTempTable("tbRecommend")
    print("done -- spark dfRecommend")
    feq_online.registerTempTable("tbFrequency_Online")
    print("done -- spark tbFrequency_Online")
    feq_offline.registerTempTable("tbFrequency_Offline")
    print("done -- spark tbFrequency_Offline")
    dfFinal_Model = saveToTempTable(Sql = sql_combine_rec)
    print("done -- dfFinal_Model")
    s3fsHandler.putFileWithBackup('Final_Model.npy', dfFinal_Model.toPandas())
    print("done -- Final_Model.npy")
    s3fsHandler.putFileWithBackup('Frequency_Offline.npy', feq_offline.toPandas())
    print("done -- Frequency_Offline.npy")

def bestSeller(bestSeller):
    filename = 'Top5Cate_Demo.npy'
    s3fsHandler.putFileWithBackup(filename, bestSeller.toPandas())

if __name__ == "__main__": 
    main()
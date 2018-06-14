from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.ml.feature import StringIndexer
from sparkHandler import saveToTempTable
import time

# Latent Factors to be made
RANK = 100
# number of iterating the process
NO_ITERATIONS = 10
# Alpha
APLHA = 0.01

class ALSModel:
    model = None
    joined_rdd = None

def runModel(Sql, TableName, Rank = RANK, No_iterations = NO_ITERATIONS, Alpha = APLHA):
    ### ALS ###
    print("start runModel")
    dfNonVariety_ALS = saveToTempTable(Sql = Sql, TableName = TableName)
    print(dfNonVariety_ALS.count())
    
    start = time.time()
    indexed_user = indexedUser(dfNonVariety_ALS)
    stop = time.time()
    print("done -- indexed_user " + str(stop-start))

    start = time.time()
    indexed_product = indexedProduct(indexed_user)
    stop = time.time()
    print("done -- indexed_product " + str(stop-start))

    start = time.time()
    ratings_NonVariety = indexed_product.rdd.map(lambda r: Rating(r.UserIdNew, r.PidNew, r.Visit))
    stop = time.time()
    print("done -- rating " + str(stop-start))

    alsModel = ALSModel()
    print("done -- create instance")
    
    start = time.time()
    alsModel.joined_rdd = indexed_product.select('UserIdNew').dropDuplicates().crossJoin(indexed_product.select('PidNew').dropDuplicates()).rdd.map(lambda x: (x[0], x[1]))
    stop = time.time()
    print("done -- cross join " + str(stop-start))

    start = time.time()
    saveToTempTable(DFObject = indexed_product, TableName='tbData')
    stop = time.time()
    print("done -- dump df_ALS to temp table " + str(stop-start))

    # ALS Implicit Model
    start = time.time()
    alsModel.model = ALS.trainImplicit(ratings_NonVariety, Rank, No_iterations, Alpha)
    stop = time.time()
    print("done -- model " + str(stop-start))
    return alsModel


def convertToDict(data, lambda_var):
    start = time.time()
    temp_data = data.rdd.map(lambda_var)
    dict_data = {}
    [dict_data.update(i) for i in temp_data.take(temp_data.count())]
    stop = time.time()
    print("convertToDict: "+str(stop-start))
    return dict_data

def indexedUser(df):
    # Index User
    indexUser = StringIndexer(inputCol="UserId", outputCol="UserIdNew")
    return (indexUser
        .fit(df)
        .transform(df)
        .selectExpr("UserId", "UserIdNew", "Pid", "Visit"))

def indexedProduct(df):
    # Index Product
    indexProduct = StringIndexer(inputCol="Pid", outputCol="PidNew")
    return (indexProduct
        .fit(df)
        .transform(df)
        .selectExpr("UserId", "UserIdNew", "Pid", 'PidNew', "Visit"))
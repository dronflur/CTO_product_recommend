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
    users = None
    products = None

def runModel(Sql, TableName, Rank = RANK, No_iterations = NO_ITERATIONS, Alpha = APLHA):
    ### ALS ###
    print("start runModel")
    dfNonVariety_ALS = saveToTempTable(Sql = Sql, TableName = TableName)
    print(dfNonVariety_ALS.count())
    
    indexed_user = indexedUser(dfNonVariety_ALS)
    print("done -- indexed_user")
    indexed_product = indexedProduct(indexed_user)
    print("done -- indexed_product")
    ratings_NonVariety = indexed_product.rdd.map(lambda r: Rating(r.UserIdNew, r.PidNew, r.Visit))
    print("done -- rating")
    print(ratings_NonVariety.count())

    alsModel = ALSModel()
    print("done -- create instance")
    # ALS Implicit Model
    alsModel.model = ALS.trainImplicit(ratings_NonVariety, Rank, No_iterations, Alpha)
    print("done -- model")
    lambda_users = lambda r: {r.UserIdNew: r.UserId}
    alsModel.users = convertToDict(indexed_product, lambda_users)
    #alsModel.users = indexed_product.select('UserId', 'UserIdNew').dropDuplicates().toPandas()
    print("done -- users")
    lambda_prods = lambda r: {r.PidNew: r.Pid}
    alsModel.products = convertToDict(indexed_product, lambda_prods)
    #alsModel.products = indexed_product.select('Pid', 'PidNew').dropDuplicates().toPandas()
    print("done -- products")
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
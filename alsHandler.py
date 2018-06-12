from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.ml.feature import StringIndexer
from sparkHandler import saveToTempTable

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
    alsModel.users = indexed_product.select('UserId', 'UserIdNew').dropDuplicates().toPandas()
    print("done -- users")
    alsModel.products = indexed_product.select('Pid', 'PidNew').dropDuplicates().toPandas()
    print("done -- products")
    return alsModel

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
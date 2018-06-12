from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Daily Delta Processing for Tableau B2S") \
    .enableHiveSupport() \
    .getOrCreate()

CSV_FORMAT = "csv"
SPARK_CSV_FORMAT = "com.databricks.spark.csv"


def saveToTempTable(Path = None, Sql = None, ObjectToDF = None, TableName = None, SavePath = None):
    df = None

    # Query
    if Path:
        df = loadFile(Path)
    elif Sql:
        df = spark.sql(Sql)
    elif ObjectToDF is not None:
        df = spark.createDataFrame(ObjectToDF)
    else:
        #TODO throw invalid query parameter exception
        df = None

    # Save
    if TableName:
        df.registerTempTable(TableName)
    elif SavePath:
        df.coalesce(1).write.format(SPARK_CSV_FORMAT).option("header", "true").save(SavePath)

    return df
    
def loadFile(Path, FileFormat = CSV_FORMAT):
    return spark.read.format(FileFormat).option("header", "true").load(Path)
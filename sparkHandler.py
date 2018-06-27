from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Daily Delta Processing for Tableau B2S") \
    .enableHiveSupport() \
    .getOrCreate()

CSV_FORMAT = "csv"
SPARK_CSV_FORMAT = "com.databricks.spark.csv"


def saveToTempTable(Path = None, IsParquet = False, Sql = None, ObjectToDF = None, DFObject = None, TableName = None, SavePath = None):
    df = None

    # Query
    if Path:
        df = loadFile(Path, IsParquet = IsParquet)
    elif Sql:
        df = spark.sql(Sql)
    elif ObjectToDF is not None:
        df = spark.createDataFrame(ObjectToDF)
    elif DFObject is not None:
        df = DFObject
    else:
        #TODO throw invalid query parameter exception
        df = None

    # Save
    if TableName:
        df.createOrReplaceTempView(TableName)
    elif SavePath:
        df.coalesce(1).write.format(SPARK_CSV_FORMAT).option("header", "true").save(SavePath)

    return df
    
def loadFile(Path, FileFormat = CSV_FORMAT, IsParquet = False):
    if IsParquet:
        return spark.read.parquet(Path)
    return spark.read.format(FileFormat).option("header", "true").load(Path)
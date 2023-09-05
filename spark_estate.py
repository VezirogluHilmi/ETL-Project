from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

bucket = "bitirme-proje"
spark.conf.set("temporaryGcsBucket", bucket)
spark.conf.set("parentProject", "tonal-nucleus-395310")

df = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "34.135.233.77:9092") \
.option("subscribe", "FDA") \
.option("failOnDataLoss", False) \
.load()

schema_data = StructType([
    StructField("serialnumber", StringType()),
    StructField("listyear", StringType()),
    StructField("daterecorded", StringType()),
    StructField("town", StringType()),
    StructField("address", StringType()),
    StructField("assessedvalue", StringType()),
    StructField("saleamount", StringType()),
    StructField("salesratio", StringType()),
    StructField("propertytype", StringType()),
    StructField("residentialtype", StringType()),
    StructField("nonusecode", StringType()),
    StructField("remarks", StringType()),
    StructField("coordinates", StringType()), 
    ])


readdf = df.select(F.from_json(df["value"].cast("string"),schema_data).alias("estate"))

df2 = readdf.select(
	"estate.serialnumber",
	"estate.listyear",
	"estate.daterecorded",
	"estate.town",
	"estate.address",
	"estate.assessedvalue",
	"estate.saleamount",
	"estate.salesratio",
	"estate.propertytype",
    "estate.residentialtype",
    "estate.nonusecode",
    "estate.remarks",
    "estate.coordinates"
)

df3 = df2 \
.withColumn("serialnumber_new", F.col("serialnumber").cast(IntegerType())) \
.withColumn("listyear_new", F.col("listyear").cast(IntegerType())) \
.withColumn("assessedvalue_new", F.col("assessedvalue").cast(FloatType())) \
.withColumn("saleamount_new", F.col("saleamount").cast(FloatType())) \
.withColumn("salesratio_new", F.col("salesratio").cast(FloatType())) \
.drop("serialnumber","listyear","assessedvalue","saleamount","salesratio")


query = df3.writeStream.outputMode("append").format("bigquery").option("table", "EstateSales.Sales2").option("checkpointLocation", "/path/to/checkpoint/dir/in/hdfs").option("credentia1sFile", "/home/primewalker/sw.json").option("failOnDataLoss", False).option("truncate", False).start().awaitTermination()


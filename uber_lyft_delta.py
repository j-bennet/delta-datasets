import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import SQLContext


if __name__ == "__main__":
    # input_dir = "s3a://coiled-datasets/uber-lyft-tlc/*.parquet"
    input_dir = "./uber-lyft-tlc/*.parquet"
    output_dir = "uber-lyft-delta"

    with SparkContext() as sc:
        sqlContext = SQLContext(sc)
        sc._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        )

        for year in range(2019, 2023):
            # let's do one year at a time
            df = sqlContext.read.parquet(input_dir)
            df = (df.where((df.request_datetime >= f"{year}-01-01") & (df.request_datetime < f"{year+1}-01-01"))
                  .withColumn("year", F.date_format(df.request_datetime, "yyyy"))
                  .withColumn("month", F.date_format(df.request_datetime, "MM"))
                  .repartition("year", "month")
                  )

            (df
             .write
             .partitionBy("year", "month")
             .mode("append")
             .format("delta").save(output_dir)
             )

        # for the last chunk, add an extra column
        df = sqlContext.read.parquet(input_dir)
        df = (df.where(df.request_datetime >= f"2023-01-01")
              .withColumn("year", F.date_format(df.request_datetime, "yyyy"))
              .withColumn("month", F.date_format(df.request_datetime, "MM"))
              .withColumn("is_roundtrip", df.PULocationID == df.DOLocationID)
              .repartition("year", "month")
              )
        (df
         .write
         .option("mergeSchema", "true")
         .partitionBy("year", "month")
         .mode("append")
         .format("delta").save(output_dir)
         )

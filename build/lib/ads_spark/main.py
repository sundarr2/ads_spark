from ads_spark.user_nextpage import ads_processing
from pyspark.sql.types import StructType, StructField, StringType
import sys

def main():
    data_Schema = [StructField('id', StringType(), False),
                   StructField('timestamp', StringType(), False),
                   StructField('visitorId', StringType(), True),
                   StructField('type', StringType(), False),
                   StructField('pageUrl', StringType(), False)]
    ads_struct = StructType(fields=data_Schema)
    # url = "https://sravanan-files.s3-us-west-1.amazonaws.com/ad-events-2018060100.tar.gz"
    # dir = "/Users/sravanan/ads_spark/data"
    a = ads_processing(sys.argv[1], sys.argv[2], ads_struct)
    a.download_data()
    df = a.create_df()
    df.show()
    a.transform(df).repartition(1).write.csv(path=dir+"/ads_result",header=True, mode="overwrite")

if __name__ == '__main__':
    main()



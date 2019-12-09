from ads_spark.user_nextpage import ads_processing
from pyspark.sql.types import StructType, StructField, StringType
import sys,os

def main():
    """ main function invoking all transformations, download -> df -> nextpage url -> write into csv"""
    try:
        #url = sys.argv[1]
        #file_dir = sys.argv[2]
        url = "https://sravanan-files.s3-us-west-1.amazonaws.com/ad-events-2018060100.tar.gz"
        file_dir = os.path.dirname(os.path.realpath(__file__))
        print(file_dir)
        data_Schema = [StructField('id', StringType(), False),
                       StructField('timestamp', StringType(), False),
                       StructField('visitorId', StringType(), True),
                       StructField('type', StringType(), False),
                       StructField('pageUrl', StringType(), False)]
        ads_struct = StructType(fields=data_Schema)
        ad_inst = ads_processing(url, file_dir, ads_struct)
        ad_inst.download_data()
        df = ad_inst.create_df()
        ad_inst.transform(df).write.csv(path=file_dir + "/ads_result", mode="overwrite")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()
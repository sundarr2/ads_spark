import requests, sys, os
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from os.path import basename
from ads_spark.spark import get_spark
from pyspark.sql.types import StructType, StructField, StringType

class ads_processing:
    def __init__(self, url, file_dir,schema):
        self.url = url
        self.schema = schema
        self.filename = basename(url)
        self.file_dir = file_dir
        self.src_file = file_dir+"/"+basename(url)
    def download_data(self):
        """ Downloads the file from url passed as command line arg"""
        try:
            response = requests.get(self.url, stream=True)
            if response.status_code == 200:
                with open(self.src_file , 'wb+') as f:
                    f.write(response.raw.read())
        except Exception as e:
            print(e)
    def create_df(self):
        """ creates data frame from the input schema."""
        return get_spark().read.json(self.src_file, schema=self.schema)
    def transform(self, df):
        """Cleanup and add new field for next url
        Assumptions after initial data analysis -
                      1) since this is about user visit journey drop all null visitorids
                      2) remove the row if there is any repeated page url in the sequence
                      before assigning next page url
                       pages(a -> a -> b -> a) => (a->b->a)
        """
        # drop null visitors and repartition by visitor to perform multiple sorting on visitor id
        df = df.na.drop(subset=['visitorId']).repartition("visitorId")
        # change datatype of timestamp to long before applying sorting
        df = df.select("id", df['timestamp'].cast('Long').alias('timestamp'), "type", "visitorId","pageURL")
        # Remove duplicate url entries per visitor in adjuscent rows
        window_spec = Window.partitionBy("visitorId").orderBy("timestamp")
        df = df.withColumn("prevUrl", F.lag("pageURL").over(window_spec)).filter("pageURL != prevUrl or prevUrl is null")
        # add next page url and display timestamp in readable format
        df = df.withColumn("nextPageUrl",F.lead("pageURL").over(window_spec))
        return(df.select("id", "timestamp", "type" , "visitorId", "pageUrl", "nextPageUrl"))

def main(args=None):
    """ main function invoking all transformations, download -> df -> nextpage url -> write into csv"""
    try:
        if args is None:
            url = "https://sravanan-files.s3-us-west-1.amazonaws.com/ad-events-2018060100.tar.gz"
            file_dir = os.path.dirname(os.path.realpath(__file__))
        else:
            url = sys.argv[1]
            file_dir = sys.argv[2]
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
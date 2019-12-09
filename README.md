# ads_spark

1) The data files are available in the sub directory called data
2) How to run
run it from script folder and pass url and dir to save the file
spark-submit --master local --driver-memory 2g --py-files  ads_spark-1.0.0-py3.7.egg ads_spark/user_nextpage.py --url "s3..." --file_dir  "/Users/sravanan/ads_spark/data"


3)transform includes some assumtions - a) remove null visitor id as we are doing visitor analysis
                                       b) nextPageUrl will have different URL from the current row, 
                                       so removed the duplicates from adjuscent rows but it will keep any repeated url after differnt page visit
                                       if user a visits pages in this order (page1-> page1->page2->page1->page3) op will have page1->page2->page1->page3
                                       
4) I couldnt get time to explore .egg file in the weekend , so submitting the working code from my local machine before deadline.
I will try to modify today.

5) I have done initial analysis using notebook and added it here. and added unit testing script done for the transformation logic

                                  

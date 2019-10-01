from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql import Row

from read_config import read_config




## to run:
##  spark-submit --driver-class-path /usr/local/spark/jars/postgresql-42.2.6.jar python_psql.py


def read_people():
    import psycopg2
    connect_str =  "host=" + HOST + " port=" + str(PORT) + " dbname=" + DB + " user=" + USER + " password=" + PASSWORD 
    
                  
                  
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()
    cursor.execute("""SELECT * from people;""")
    rows = cursor.fetchall()
    print("-----------------")
    print(rows)
    print("-----------------")
    cursor.close()
    conn.close()
    return None



def merp(spark):
    rdd_df = spark.read.json("/usr/local/spark/examples/src/main/resources/people.json")
    # Displays the content of the DataFrame to stdout
    rdd_df.show()

    mode = "overwrite"
    url = "jdbc:postgresql://" + HOST + ':' + str(PORT) + '/'
    properties = {"user": USER,"password": PASSWORD}
    rdd_df.write.jdbc(url=url, table=TABLE, mode=mode, properties=properties)
    return None


if __name__ == "__main__":
    HOST, PORT, DB, USER, PASSWORD = read_config('psql.config')
    TABLE = "public.people"
        
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()

    merp(spark)

    spark.stop()
    read_people()

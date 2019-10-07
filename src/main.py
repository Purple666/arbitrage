from arbitrage import pipeline
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    workqueue = []
    for month in range(108, 109):
        for year in range(2010, 2011):
            YEAR = str(year)
            MONTH = str(month)[1:]
            print('year: ', YEAR, 'month: ', MONTH)
            workqueue.append((YEAR, MONTH))
    workqueue = sc.parallelize(workqueue, 1)

    workqueue.foreach(lambda x: pipeline(*x))

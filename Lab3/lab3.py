from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row

conf = SparkConf().setAppName('InverseIndex').setMaster('yarn-cluster')
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

ssc = StreamingContext(sc, 20)
sqlc = SQLContext(sc)

def rddWordCountFunc(rdd):
    if (rdd.count() > 0):
        rdd = rdd.flatMap(lambda t: t[1]).map(lambda x: Row(word=x))
        wordschema = sqlc.createDataFrame(rdd)
        wordschema.createOrReplaceTempView("words")
        query = 'SELECT word, COUNT(word) as cnt '\
                'FROM words '\
                'GROUP BY word '\
                'ORDER BY cnt DESC'
        wc = sqlc.sql(query)
        wc = wc.limit(10)
        print 'Top words: '
        wc.show()

lines = ssc.socketTextStream("localhost", 9999)

words = lines.window(120, 20)

words.count().map(lambda x: 'Tweets: ' + str(x)).pprint()

words = words.flatMap(lambda line: line.split(" ")).map(lambda word: (1, [word])).reduceByKey(lambda x, y: x + y)
words.foreachRDD(rddWordCountFunc)

ssc.start()
ssc.awaitTermination()
from pyspark import SparkContext, SparkConf
import string
import unicodedata
import sys

conf = SparkConf().setAppName('InverseIndex').setMaster('yarn-cluster')
sc = SparkContext(conf=conf)

tbl = dict.fromkeys([i for i in range(sys.maxunicode)
                      if not unicodedata.category(unichr(i)).startswith('L') and unichr(i) != u"'"], u' ')
def remove_punctuation(text):
    return text.translate(tbl)
    
def map_to_csv(x):
	to_ret = "" + str(x[0]) + "," + str(x[1][0]) + ","
	for doc_name in x[1][1]:
		to_ret += str(doc_name) + " "
	return to_ret
	
    
#print unicodedata.category(u'>')

text_files = sc.wholeTextFiles('hdfs:///20_newsgroup/*/*') \
			   .map(lambda x: (x[0].split('/')[-1], x[1])) \
			   .flatMapValues(lambda x: x.split('\n')) \
			   .filter(lambda x: x[1] != '' and 
					(x[1].find(' ') == -1 or x[1].find(':') == -1 or x[1].index(' ') < x[1].index(':'))) \
			   .mapValues(lambda x: remove_punctuation(x).lower()) \
			   .flatMapValues(lambda x: x.split()) \
			   .map(lambda x: (x[1], (1, set([x[0]])))) \
			   .reduceByKey(lambda x, y: (x[0] + y[0], x[1].union(y[1]))) \
			   .map(map_to_csv)

text_files.saveAsTextFile('hdfs:///inverse_index.csv')

#out = open('output.txt', 'w')

#for x in text_files:
	#out.write(str(x) + "\n")

#out.write(unicodedata.category(u'@'))

#out.close()

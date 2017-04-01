from pyspark import SparkContext, SparkConf
from operator import add

conf = SparkConf().setAppName('Triangles').setMaster('yarn-cluster')
sc = SparkContext(conf=conf)

text_file = sc.textFile("hdfs:/graph.txt")

def mapEdges(word):
	split_res = word.split(" ")
	return [(split_res[0], split_res[1]), (split_res[1], split_res[0])]

edges_all = text_file.flatMap(mapEdges).distinct()

edges_to_triangle = edges_all.join(edges_all) \
		.filter(lambda x: x[1][0] != x[1][1]) \
		.values() \
		.map(lambda x: (x[0] + '-' + x[1], 1))\
		.reduceByKey(lambda x, y: x + y)
		
trian_num = edges_all.map(lambda x: (x[0] + '-' + x[1], -1)) \
		.join(edges_to_triangle) \
		.values() \
		.map(lambda x: x[1]) \
		.reduce(add)
trian_num /= 6

out = open('output.txt', 'w')

#for x in edges_all.collect():
out.write(str(trian_num) + "\n")

out.close()

print trian_num

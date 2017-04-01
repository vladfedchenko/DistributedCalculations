from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('InverseIndex').setMaster('yarn-cluster')
sc = SparkContext(conf=conf)

text_files

out = open('output.txt', 'w')

#for x in edges_all.collect():
out.write(str(trian_num) + "\n")

out.close()

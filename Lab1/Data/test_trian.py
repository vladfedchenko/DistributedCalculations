f = open('graph.txt', 'r')
edges_all = []
print edges_all

line = f.readline()
while line != '':
    split_res = line.split(" ")
    tuple1 = (int(split_res[0]), int(split_res[1]))
    tuple2 = (int(split_res[1]), int(split_res[0]))
    if ((tuple1 in edges_all) or (tuple2 in edges_all)):
        print "Edge copy detected."
    else:
        edges_all.append(tuple1)
        edges_all.append(tuple2)
    line = f.readline()

trian_num = 0;
for x, y in edges_all:
    #print (x, y)
    for y1, z1 in edges_all:
        if (y == y1):
            for z, x1 in edges_all:
                if (z1 == z and x1 == x):
                    trian_num += 1

print trian_num / 6

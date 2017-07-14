from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import itertools
from itertools import chain,combinations
from collections import defaultdict
import sys
from operator import add
import copy
import time
t= time.time()

def phaseaaaa(inputphase1, lenLine, combinations):
    linesnum = list(inputphase1)
    res = []
    partitionSupport = (minSupport * float(len(linesnum))/float(lenLine))
    for i in combinations:
        count = 0
        for bas in linesnum:
            t = set(i)
            for j in i:
                if j in bas:
                    t.remove(j)
            if not t:
                count += 1
        res.append((i,count))

    yield res
def generate_candidate(freqset, itemSize):
    count = {}
    candidate = []
    freqsetList = freqset.collect()
    for i in freqsetList:
        for j in freqsetList:
            x = tuple(sorted(set(i+j)))
            if x in count:
                count[x] += 1
            else:
                count[x] = 1
    for c in count:
        if len(c) == itemSize and count[c] >= (itemSize-1) * itemSize / 2:
            candidate.append(c)


    return candidate

def generate_combinations(freqset, candidate, item_size):
    res = []
    com=list(itertools.combinations(sorted(candidate), item_size))
    if item_size>2:
        a= list(freqset)
        
        for item in com:
            for i in range(item_size):
                if item[:i]+item[i+1:] not in a:
                    com.remove(item)
                    break     

    return com

def generate_freqset(combinations):
    freqset = inputlinegroupbykey.mapPartitions(lambda x : phaseaaaa(x, inputnum, combinations)).flatMap(lambda x: x).reduceByKey(add).filter(lambda x: x[1]>=minSupport).map(lambda x: x[0])

    return freqset

conf = SparkConf()
sc = SparkContext(conf = conf)


file = open('Po-Chuan_Tseng_SON_MovieLens.Big.case2-3000.txt', 'w')
lines = sc.textFile(sys.argv[2], 4)
numPartitions = lines.getNumPartitions()

header = lines.first()
lines = lines.filter(lambda line:line!= header)
minSupport = int(sys.argv[3])


if (sys.argv[1] == "1"):
    inputline1 = lines.map(lambda line: line.split(',')).map(lambda line: (int(line[0]), int(line[1])))

else:
    inputline1 = lines.map(lambda line: line.split(',')).map(lambda line: (int(line[1]), int(line[0])))


inputlinegroupbykey = inputline1.groupByKey().sortByKey().map(lambda x: list(x[1]))
inputnum = inputlinegroupbykey.count()

rdd2 = inputline1.distinct().groupByKey().flatMap(lambda x: list(x[1])).map(lambda x: (x,1)).reduceByKey(add).filter(lambda x: x[1]>=minSupport).map(lambda x: x[0]).persist()
list2 = rdd2.collect()
list2.sort()
listttt=map(lambda x:'('+str(x)+')',list2)



file.write(str(','.join(listttt)) + "\n" +"\n")
print sorted(list2)

com=list(itertools.combinations(list2, 2))


freqset = inputlinegroupbykey.mapPartitions(lambda x : phaseaaaa(x,inputnum, com)).flatMap(lambda x: x).reduceByKey(add).filter(lambda x: x[1]>=minSupport).map(lambda x: x[0])#.countByKey()


rdd2=rdd2.map(lambda x : sorted(list(set(x).intersect(freqset.collect()))))


item_size=2
stoppingCondition = False
freqitemset= {}

while not stoppingCondition:

    candidate = generate_candidate(freqset, item_size)

    freqset = generate_freqset(candidate)

    if freqset.collect() ==[]:
        stoppingCondition = True
    else:
        item_size += 1
    print sorted(freqset.collect())
    freq=map(lambda x :str(x),freqset.collect())
    lst=str(','.join(sorted(freq)))
    file.write( lst + "\n" +"\n")

print "DOne!", time.time()-t




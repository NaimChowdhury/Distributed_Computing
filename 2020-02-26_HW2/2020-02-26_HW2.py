import random as rand


## Problem A
useless = sc.parallelize(range(1000000))

def mapping(x):
    row = [rand.randint(1,6) for i in range(1,8)]
    if sum( row[0:5] ) > sum( row[5:8] ):
        return (1,1)
    else:
        return (0,1)

def add(x,y):
    return x+y

useful = useless.map(mapping)

count = useful.reduceByKey(add)

probability = count.collect()[1][1] / (count.collect()[0][1] + count.collect()[1][1])

# Problem B
set1 = sc.textFile("/usr/examples/Popular_Baby_Names.csv")

set1header = set1.first()
header = sc.parallelize([set1header])

set1 = set1.subtract(header)

def aggregate(x):
    row = x.split(',')
    return ( (row[3].lower(),row[1]),  int(row[4]) ) 

set2 = set1.map(aggregate)

set3 = set2.reduceByKey(add)

set3.collect()




import random as rand


## Problem A
useless = sc.parallelize(range(100000))

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


import random as rand


# Warm-up Exercises

## Compute the Average of the numbers in Some_Numbers.csv

someNumbers =sc.textFile("/usr/examples/Some_Numbers.csv") 

def average(x): # n -> (n,1)
    return (int(x), 1)

def sumItUp(x,y): # (x_0 + y0, x_1 + y_1)
    return (x[0]+y[0], x[1] + y[1])

sumNumbers = someNumbers.map(average)

avgNumbers = sumNumbers.reduce(sumItUp)

avg = avgNumbers[0] / avgNumbers[1]

print('The average of the numbers is: ', avg)


## Compute the sum of the even numbers in Some_numbers.csv

def isEven(x):
    if x%2==0:
        return int(x)
    else:
        return 0

def add(x,y):
    return x+y

evens = someNumbers.map(isEven)

sumEvens = evens.reduce(add)

print('The sum of the even numbers is: ', sumEvens)




# Problem A
useless = sc.parallelize(range(1000000))

def mapping(x): #produces dataset of 7 dice rolls per row 
    row = [rand.randint(1,6) for i in range(1,8)]
    if sum( row[0:4] ) > sum( row[4:7] ): # checks sum for each experiment
        return (1,1) #success
    else:
        return (0,1) #failure

useful = useless.map(mapping)

count = useful.reduceByKey(add)

probability = count.collect()[1][1] / (count.collect()[0][1] + count.collect()[1][1])

print("The probability of the sume of the first set of 4 dice rolls being larger than the second set is: ", probability)

# Problem B

# If we don't care about the ethnicity, how can we use spark to remove repetition?
set1 = sc.textFile("/usr/examples/Popular_Baby_Names.csv")

set1header = set1.first()
header = sc.parallelize([set1header])

set1 = set1.subtract(header)

def aggregate(x):
    row = x.split(',') # turns string into list split by commas
    return ( (row[3].lower(),row[1]),  int(row[4]) ) #return only ((name, gender), count)

set2 = set1.map(aggregate) 

set3 = set2.reduceByKey(add) 

set3.collect() # Final result, with ((name, gender), count)

## What is the most common name for boys?

def maximum(a,b):
    return max(a[1], b[1])

def malecheck(a):
    if a[0][1] == 'MALE':
        return True
    else:
        return False

boys = set3.filter(malecheck)

popularBoy = boys.max(lambda x:x[1])

print('The most common name for boys is ', popularBoy)

## What is the most common name for girls?
def femalecheck(a):
    if a[0][1] == 'FEMALE':
        return True
    else:
        return False

girls = set3.filter(femalecheck)

popularGirl = girls.max(lambda x:x[1])
print('The most common name for girls is ', popularGirl)

## What percentage of all boys are named Jacob? What percentage of all girls are named Emma?
def total(x,y):
    return (('total'), x[1] + y[1])

totalboys = boys.reduce(total)
percOfJacobs = 100*popularBoy[1]/totalboys[1]

print('The percentage of all boys named Jacob is ', percOfJacobs, '%.')

totalgirls = girls.reduce(total)
percOfEmmas = 100*popularGirl[1]/totalgirls[1]

print('The percentage of all girls named Emma is ', percOfEmmas, '%.')

# How many names are given to both boys and girls?
def nameMap(x):
    return (x[0][0], 1)


namesOnly = set3.map(nameMap)

genderNeutral = namesOnly.reduceByKey(add)

genderNeutral = genderNeutral.filter(lambda x: x[1]==2)

genderNeutral.collect()


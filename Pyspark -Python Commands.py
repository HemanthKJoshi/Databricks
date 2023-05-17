# Databricks notebook source
df = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")

) 

# COMMAND ----------

df.write.format("delta").saveastable("geotable")

# COMMAND ----------

create table using delta location ('/mnt/delta/gCREATE TABLE diamonds USING DELTA LOCATION '/mnt/delta/diamonds/'eo')

# COMMAND ----------

CREATE TABLE geo USING DELTA LOCATION ("/databricks-datasets/samples/population-vs-price/data_geo.csv")
 

# COMMAND ----------

df.show()

# COMMAND ----------

df.sort("2015 median sales price").asc().show()

# COMMAND ----------

from pyspark.sql.functions import asc
df.sort(df('State Code').asc()).display()

df.show

# COMMAND ----------

df.show()

# COMMAND ----------

dfgrp=df.groupBy("state")
dfgrp.show()

# COMMAND ----------

from pyspark.sql.functions import show
dfgrp display()

# COMMAND ----------

df.show()
df2=df.groupby("State")
df2.show()

# COMMAND ----------

df.groupby('State').sum('2015 median sales price').show()

# COMMAND ----------

df.orderby(col('State')).show()

# COMMAND ----------

dfordby=df.orderBy(('State')).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC
# MAGIC CREATE TABLE diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %python
# MAGIC df1 = (spark.read
# MAGIC   .format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
# MAGIC )
# MAGIC
# MAGIC df1.write.format("delta").save("/mnt/delta/diamonds")

# COMMAND ----------

df1.show()

# COMMAND ----------

num_1=2
num_2=3


# COMMAND ----------

print(num1)


# COMMAND ----------

print(num1 + num2)

# COMMAND ----------

print(num_1 + num_2)
total = num_1 + num_2
total

# COMMAND ----------

total

# COMMAND ----------

print (num_1)

num1 =

# COMMAND ----------

str1 = 'Hello World'
str1 = str1.split()
str1[1]

# COMMAND ----------

str1[0]

# COMMAND ----------

str1.append('New')
str1


# COMMAND ----------


# tuples Examples

tuple1 = (1,2,3,4,5)
tuple2 = ('a','b','c','d','e')
zipped= zip (tuple1,tuple2)
print (zipped)
result = tuple (zipped)
result
tuple1[-5]



# COMMAND ----------

Bikeowners = {'James' : 'Enfeld', 'Henry' : 'Hero Honda', 'Jack' :  'Pulsar'}
Bikeowners['Henry']

# COMMAND ----------

Bikeowners.keys()
'Jack' in Bikeowners


# COMMAND ----------

#
# For Loop example

for x in range(2):
  print(x)

# COMMAND ----------

for x in range(2,30,3):
    print(x)

# COMMAND ----------

for x in range(6):
  if x == 3: break
  print(x)
else:
  print("Finally finished!")

# COMMAND ----------

adj = ["red", "big", "tasty"]
fruits = ["apple", "banana", "cherry"]

for x in adj:
  for y in fruits:
    print(x, y)

# COMMAND ----------

for x in [0, 1, 2]:
  pass

# COMMAND ----------

#Function Examples

def my_function():
  print("Hello from a function")

# COMMAND ----------

def my_function():
  print("Hello from a function")
  my_function()

# COMMAND ----------

def my_function(fname):
  print(fname + " Refsnes")
  my_function('Jack')

# COMMAND ----------

my_function('Jill')

# COMMAND ----------

def my_function(fname, lname):
  print(fname + " " + lname)

my_function("Emil", "Refsnes")

# COMMAND ----------


def my_function(fname, lname):
  print(fname + " " + lname)

my_function("Emil", "Refsnes")


# COMMAND ----------



# COMMAND ----------

def myfn1(*args):
 print("min number is:" + args[4])
myfn1("1","2","3","4","5")

# COMMAND ----------

def myfn2(ch1,ch2,ch3):
 print("last mode is " + ch2)
myfn2(ch1="22",ch2="34",ch3="45")


# COMMAND ----------

def myfn(x):
    return 25 * x
    print( "first return value " + myfn(2))
    print(myfn(4))

# COMMAND ----------

def myfn2(x):
 return 5 * x
 print("first return value " + myfn2(10))
 print("first return value " + myfn2(10))

# COMMAND ----------

myfn2(10)
myfn2(23)
myfn2(11)

# COMMAND ----------

def tri_recursion(k):
  if(k > 0):
    result = k + tri_recursion(k - 1)
    print(result)
  else:
    result = 0
  return result

print("\n\nRecursion Example Results")
tri_recursion(6)

# COMMAND ----------

# lambda funciton examples


x = lambda a : (a*a) + 10
 print(x(2))
 

# COMMAND ----------

x = lambda a,b : (a*b) + (a-b)
 print(x(2,3))
 

# COMMAND ----------

x = lambda a,b,c : (a*b+c) -(a-b+c)
 print(x(2,3,4))

# COMMAND ----------

print(x(2,3,3))

# COMMAND ----------



# COMMAND ----------

# lambda with Funcitons
def myfunc(n):
  return lambda a : a * n
  double = myfunc(3)
  double(3)
 


# COMMAND ----------

# if-else loop

a = 10
b = 10
if a > b :
    print("a is greater than b")
elif b > a:
    print("b is greater than a")
else :
    print(" all are equal")
 

# COMMAND ----------

# while loop

i = 1
while i < 5:
  print(i)
  if i == 3 :
    break
  i += 1

# COMMAND ----------

# while loop

i = 0
while i < 6:
  i += 1
  if i == 3:
    continue
  print(i)

# COMMAND ----------

Array1 = ["a","b","c","d","e","f","g"]


Array1.append("g")

Array1.pop(6)
Array1.reverse()
Array1.count("b")
Array1.sort()
Array1

# COMMAND ----------

def myFunc(e):
  return len(e)

cars = ['Ford', 'Mitsubishi', 'BMW', 'VW']

cars.sort(key = myFunc)
cars


# COMMAND ----------

# Classes

class MyClass:
 y = 500
print (MyClass().y)

# COMMAND ----------



# COMMAND ----------

class person11:
    def __init__(self,name,age):
     self.name = name
     self.age = age
p11 = person11('Jack', 48)   
print(p11.age)

# COMMAND ----------

class Person:
  def __init__(self, name, age):
    self.name = name
    self.age = age

p1 = Person("John", 36)

print(p1.name)
print(p1.age)

# COMMAND ----------

class Person:
  def __init__(self, name, age):
    self.name = name
    self.age = age

p1 = Person("John", 36)

print(p1)

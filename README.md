# Spark-Database-Tools-Practice
## [Calculating The Average Friends By Age In A Social Network](https://github.com/phuongnhhe150172/Spark-Database-Tools-Practice/tree/main/FriendsByAge/FriendsByAge)
This assignment I worked with Spark RDD, pair RDD
### Problem Description:
Create an object to list the average friends of each age
#### Strategy:
* Read file data source to an rdd
* parsLine to pair RDD
* Calculating each age has total friends and total number of age is equal
* Calculating average friends of each age

## [Calculating The Most Obscure Superheroes DataSet](https://github.com/phuongnhhe150172/Spark-Database-Tools-Practice/tree/main/MostObscureSuperheroDataSet/MostObscureSuperheroDataSet)
### Objectives:
This assignment I worked with Spark SQL, DataFrames & DataSet
### Problem Description:
Create an object to list the names of all superheroes with only ONE connection
#### Strategy:
* Can be large unchanged until to point where the "connections" dataset is constructed
* Filter the connections the find rows with only one connection
* Join Superheroes with Connections dataset to find result
* Select the names column and show it

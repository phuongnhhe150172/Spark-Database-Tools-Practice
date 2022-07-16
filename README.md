# Spark-Database-Tools-Practice
## [Calculating The Average Friends By Age In A Social Network](https://github.com/phuongnhhe150172/Spark-Database-Tools-Practice/tree/main/FriendsByAge/FriendsByAge)
### Objectives:
This assignment I worked with Spark RDD, pair RDD
### Problem Description:
Create an object to list the average friends of each age
#### Strategy:
* Read file data source to an rdd
* parsLine to pair RDD
* Calculating each age has total friends and total number of age is equal
* Calculating average friends of each age

## [Calculate Top1 Ranking Football Of Bundesliga(D1) between 2000-2010](https://github.com/phuongnhhe150172/Spark-Database-Tools-Practice/blob/main/Jupyter/data/RankingFootball.ipynb)
### Objectives:
This assignment I worked with PySpark by Jupyter Notebook on docker platform
### Problem Description:
* Read data from the file "/.Data/soccer/matches.csv"
* Calculate top 1 ranking football of Bundesliga (D1 = 1) and Season from 2000 to 2010
#### Strategy:
* Determine each matches is win, loss or tie using withColumn ('HomeTeamWin', 'AwayTeamWin', 'GameTie') 
* Filter Div = D1 and Season from 2000 to 2010
* Collect information of home matches of all team on each season (Season, Team, TotalHomeWin, TotalHomeLoss, TotalHomeTie, HomeScoredGoals, HomeAgainstGoals)
* Collect information of away matches of all team on each season (Season, Team, TotalAwayWin, TotalAwayLoss, TotalAwayTie, AwayScoredGoals, AwayAgainstGoals)
* Collect homa matches and away matches of all team on each season (Season, Team, GoalsScored, GoalsAgainst, GoalDifferentials, Win, Loss, Tie, WinPct)
* Calculating raking of each team on each season by window partition by season and order season "asc"
* Show top1 ranking football

## [Cohort Analysis](https://github.com/phuongnhhe150172/Spark-Database-Tools-Practice/blob/main/Jupyter/data/DailyFristPayment.ipynb)
### Objectives:
This assignment I worked with PySpark by Jupyter Notebook on docker platform
### Problem Description:
* Read data from the file "/.Data/order/online-retail.csv"
* Calculate the retention of customers from their first payment
#### Strategy:
* Select column InvoiceDate and CustomerID from online_retail_df
* Find first payment date of each customer
* Calculating total new customers daily
* Calculating retained customer following the day after the first payment date
* Join two results to display number of retained customer by the following day

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

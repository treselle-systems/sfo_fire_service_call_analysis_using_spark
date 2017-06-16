Spark on YARN – Performance and bottlenecks

About Dataset
SFO Fire Calls-For-Service dataset includes all fire units responses to calls. This dataset has 34 columns and 4.36 Million of rows while us doing this use case. This dataset has been updating on daily basis. 

About Hadoop Cluster
We have set it up 2 node hadoop cluster using the HDP 2.6 distribution. This distribution comes with Spark 2.1 and which is used for our use case Spark application execution.
Instance details: m4.xlarge (4 cores, 16 GB RAM)

About Use Case
To understand the Spark performance and tuning the application we have created Spark application using RDD, DataFrame, Spark SQL and Dataset APIs to answer the below questions from the SFO Fire department call service dataset.
1.	How many different types of calls were made to the Fire Department?
2.	How many incidents of each call type were there?
3.	How many years of Fire Service Calls are in the data file?
4.	How many service calls were logged in the past 7 days?
5.	Which neighborhood in SF generated the most calls last year?
Out of the first question, to answer for all the remaining questions we have to group the data (in terms of Spark it is nothing but data shuffle). 

Please refer our blog post for more details http://www.treselle.com/blog/

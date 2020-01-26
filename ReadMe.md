Eurowings Digital - Coding challenge

Task Description
As part of our initiative to transform Eurowings into a booking platform, we will be offering hotels and apartments through our platform. Imagine 2 years from now, that we have been hugely successful in offering apartments (like AirBnb) on our website. In order to offer the best price, we want to come up with a price prediction model, which uses the historical data of our booking platform.
Task
You get from us a data sample which contains historical listings from AirBnb. However, in its current state, it is not suitable to be used right away by our data scientists who want to focus on the model building without having to modify the data much further. Your task is now the following:
Overall Goal: Prepare the provided dataset for deriving a price prediction model.
Sub Tasks:
Document thoroughly your approach for manipulating the dataset.
Document how you intend your code to interface with the source system and the data scientists' models and how the data could be continuously updated (make assumptions where necessary).
Document how your code could be scaled if confronted with terabytes of data. Alternatively, implement mechanisms that allow seamless scaling.
Bonus Point:
Use the processed data to derive a price prediction model.

Submission


Overview
Designing a daily batch processing pipeline that can evaluate the input and clean them for further usage. This pipeline should be able to be automated and parameterized. 

Usage
Requirements
Sbt : Installation
Airflow: Installation
Spark: Installation
csvtool: sudo apt-get install csvtool
After installing the requirements
bash extra/bash/config.sh


Pipelines
Two pipelines were designed for this challenge. The first one prepare the data for the model and other use cases and the other pipeline train the model.

1- ETL Pipeline: 



Data ingestion: In this step all the csv files in the provided path will be read and parsed to a spark dataframe
Csv tools: In order to evaluate the number of rows parsed by spark, csv tools is used to compare the row count of input 
Data evaluation: As the quality of data are very important, this task will evaluate not only the schema but also the values of fields. I explain this part with more detail in the next chapters.
Data preparation: data will be prepared for not only the model but also other use cases such as Data warehousing or data exploration or analyzation 
Feature engineering data preparation: data will be prepared specifically for the ML model. For example, the columns which are not required for the model will be dropped or some important fields which contain null, Zero or empty values will be also dropped.  




2- ML Pipeline:


Note: This pipeline is only a demo to present how we can continuously train a model based on the historical data
Data segregation: all the required ML functions will be applied on the data and will be splitted to train and test
Model training: The model or models serve the test and train. A Metric report will be generated next to the model.  
Model deployment: Model will be saved with a new version.  














Data Ingestion
Data quality
Row count check: As each line of data is important for us we need to be sure if pipeline will ingest the whole data and would not skip or forget some lines. Therefore using some external tools, I count the rows number and compare them with the rows number which pipeline parsed and in case of differences task will be failed.

Data structure: Because it is very important to have a reliable structure of data, the new incoming data structures should be checked with what we already have. For that reason the schema has been defined in a json format and been placed in the resources of the project. The ingestion task,  after reading and parsing the CSVs to spark dataframe check the dataframe schema against the already defined one and will reject the ingestion in case of differences in column’s types or number of columns.

Data integrity: can be considered a crucial part of data quality checks. One of the most important areas to check is whether the ingested raw data is correct. 
There are lots of different methods to check the data quality. In this task I used Deequ which is an open source tool developed and used at Amazon. It is built on top of Apache Spark  and measure data quality in large datasets. It can handle the following dimensions: completeness measures, uniqueness looks, Timeliness ensures, validity measures, accuracy measures, consistency evaluates
In this task only some of the measurements were used to demonstrate this step. To make this part easy for the production development, the fields and their measurements were defined in a config file in json format which is placed under resources folder. For example, the following rules will check the id values against the null or empty and the other rule means id should not have any negative values:
{ "field": "id", "rule": "Complete", "type": "StringType", "extra": ""    },
{ "field": "id", "rule": "NonNegative", "type": "StringType", "extra": ""    }

Data format
After parsing the data to spark dataframe we should save them to a local or cloud storage. In order to increase the performance I used parquet format. 
Partitioning strategy
Partitioning will improve scalability and performance for the large scale data. However, the partitioning strategy must be chosen carefully. As we have a daily batch pipeline and also model will take the data time best the best strategy is to partition the data by date. Most common partitioning case is year/month/day but in order to query this data in the future by some data catalog tools such as Hive or spectrum people partition there will be some cases in which this partitioning can not be helpful. Assume that we want to query some data between this date ranges 28-01-2019 to 03-02-2019 then it should be parsed to this query: select * from table where year=2019 and (month =1 or month =2) and (day >=28 and day <= 03). Therefore the standard strategy would be year/yearMonth/yearMonthDay  year=2018/month=201801/day=20180101.
Data preparation

Regardless of our assumption for the usage of the data, in this task we prepare the data in a reliable and standard format for further uses. 
Boolean fields: in the new coming data boolean values are defined by t or f which this should be converted to true false and column type should be casted to a boolean.
Non values fields: Some fields are holding none which is equivalent to null in spark dataframe and this should be converted to null.
Removing brackets and parentheses: list values were wrapped into bracket, parentheses or curved brackets. Using regex, I remove these and split the values into array and cast that field to an array. This can help us in future to be able query or filter this values faster.
Trimming big text: \r \n or \t were replaced by [NewLine] and [Tab], to be able to visualize the data frame.
Numeric fields: $ and % were removed from the numeric fields and were casted to Double type.
 
Model data preparation

	NLP
	Extra fields



Continuous Delivery for Machine Learning
	- Persistent
	- Data
		- Schema
		- Sampling over time
		- Volume
	- Model
		- Algorithm
		- More training
		- Experiments
	- Code
		- Business needs
		- Bug fixes
		- Configuration
	- In order to formalise the model training process in code, we used an open source MLFlow
	

	https://martinfowler.com/articles/cd4ml.html


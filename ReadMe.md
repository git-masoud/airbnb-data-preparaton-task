# Data engineer task challenge



## Task Description
 As part of our initiative to transform our project into a booking platform, we will be offering hotels and apartments through our platform. Imagine 2 years from now, that we have been hugely successful in offering apartments (like AirBnb) on our website. In order to offer the best price, we want to come up with a price prediction model, which uses the historical data of our booking platform.
* Task
    * You get from us a data sample which contains historical listings from AirBnb. However, in its current state, it is not suitable to be used right away by our data scientists who want to focus on the model building without having to modify the data much further. Your task is now the following:
        Overall Goal: Prepare the provided dataset for deriving a price prediction model.
* Sub Tasks:
    * Document thoroughly your approach for manipulating the dataset.
    * Document how you intend your code to interface with the source system and the data scientists' models and how the data could be continuously updated (make assumptions where necessary).
    * Document how your code could be scaled if confronted with terabytes of data. Alternatively, implement mechanisms that allow seamless scaling.
* Bonus Point:
    * Use the processed data to derive a price prediction model.
         
## Submission

 [Price prediction data pipeline](https://github.com/git-masoud/airbnb-data-preparaton-task/blob/master/extra/Price%20prediction%20data%20pipeline.pdf)

### Installation and Execution

### Getting Started
   These instructions will make you two pipelines in airflow which they would run on your local machine.
### Prerequisites


   Java Version 8   
   
   Scala Version 2.11.9   
   
   [sbt](https://www.scala-sbt.org/release/docs/Installing-sbt-on-Linux.html&sa=D&ust=1580247924553000)
   
   [Airflow](https://airflow.apache.org/docs/stable/installation.html&sa=D&ust=1580247924553000)
   
   [Spark](https://spark.apache.org/docs/latest/index.html&sa=D&ust=1580247924553000)
   
   csvtool ```sudo apt-get install csvtool```

  
### Execution
   Clone the project from: 
    ```git clone https://github.com/git-masoud/airbnb-data-preparaton-task.git ```
   
   Navigate to the project root and run config bash script:
   
``` 
bash extra/bash/config.sh 
export AIRFLOW_DAG_PATH=[Path of airflow]/dags 
```

   This will download the data, configure airflow, run airflow webserver and airflow scheduler. After that by opening airflow UI you should be able to find airbnb_data_preparation_demo and airbnb_Ml_pipeline_demo. 
   You can trigger them and the rest will be done by the pipeline.
   

## 5. Project Structure

``` 
── airflow
    └── airbnb_pipeline.py
    └── airbnb_model_pipeline.py
── data
    └── airbnb
── extra
    └── deequ-1.0.2.jar
    └── testproject.jar
── src
    └── main
        └── scala
            └── task
                └── common
                    └── Constants
                    └── DataQaulity
                    └── SparkUtils
                └── main
                    └── DataIngestor 
                    └── ListingsModelDataPreparation 
                    └── ListingsTablePreprocessor 
                    └── MLListingsModel
    └── test
        └── scala
            └── task
                └── common
                    └── SparkUtilsTests
── build.sbt
── .gitignore
── config.sh
── ReadMe.md
``` 


### TODOs
- Implementing the ml part properly
- All the parts should be more dynamic. No hard code!
- Write more test case 
- dockerising the project
- Add night mode ;)

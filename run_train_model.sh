start_date=$(date +"%Y-%m-%d" -d "$days_before day ago")
end_date=$(date +"%Y-%m-%d")
spark-submit --class task.main.MLListingsModel $spark_config target/scala-2.11/testproject.jar file:///$model_data_path file:///$model_path $start_date $end_date
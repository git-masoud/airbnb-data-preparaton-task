echo $csv_path
#sudo apt-get install csvtool
number_of_lines=$(csvtool height $csv_path/$table_name*.csv)
number_of_files=$(find $csv_path/ -type f | wc -l)
#subtracting the header from lines
number_of_rows="$(($number_of_lines-$number_of_files))"
spark-submit --class task.main.DataIngestor $spark_config target/scala-2.11/testproject.jar file:///$csv_path file:///$staging_path $table_name $number_of_rows
echo masoud
echo $spark_app_id
echo $app_id
echo masoud
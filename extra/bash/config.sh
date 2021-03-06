set -e

echo Starting Apache Airflow with command:
echo airflow "$@"

exec airflow "$@"
project_path=$(pwd)/
data_path=$project_path/data/airbnb/
#download source files
mkdir -p $data_path/csvs_files
cd $data_path 
wget https://recruitingupload.blob.core.windows.net/public20190906/listings.tar.gz
tar xvzf listings.tar.gz -C $data_path/csvs_files/
airflow variables -s extra_jars "$project_path/extra/deequ-1.0.2.jar"
airflow variables -s project_path "$project_path/"
airflow variables -s project_file_path "$project_path/extra/testproject.jar"
airflow variables -s csv_path "$data_path/csvs_files"
airflow variables -s staging_path "$data_path/staging"
airflow variables -s preproccessed_path "$data_path/preprocessed"
airflow variables -s model_data_path "$data_path/model_data"
airflow variables -s model_path "$data_path/ml_model"
airflow variables -s table_name "listings"
airflow variables -s run_date "all"
airflow variables -s days_before 2000

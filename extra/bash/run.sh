chmod +x ./run_staging.sh
chmod +x ./run_preprocessing.sh
chmod +x ./run_model_data_preparation.sh
chmod +x ./run_train_model.sh
export spark_config="--master local[*] --jars extra/*.jar"
export project_file_path="/home/masoud/projects/testProject/extra/testproject.jar"
export csv_path="/home/masoud/data/airbnb/raw_files"
export staging_path="/home/masoud/data/airbnb/staging"
export preproccessed_path="/home/masoud/data/airbnb/preprocessed"
export model_data_path="/home/masoud/data/airbnb/model_data"
export model_path="/home/masoud/data/airbnb/ml_model"
export table_name="listings"
export run_date="all"
export days_before=2000
if [[ $1 == 1 ]]
then
./run_staging.sh
elif [[ $1 == 2 ]]
then
./run_preprocessing.sh
elif [[ $1 == 3 ]]
then
./run_model_data_preparation.sh
elif [[ $1 == 4 ]]
then
./run_train_model.sh
else
echo "run.sh mode(1,2,3)"
fi
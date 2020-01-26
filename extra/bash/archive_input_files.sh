if [ ! -d $csv_path/archive ]; then
mkdir $csv_path/archive; 
fi
if [ ! -d $csv_path/archive/$(date +"%Y-%m-%d") ]; then
mkdir $csv_path/archive/$(date +"%Y-%m-%d");
fi 
mv $csv_path/*.csv $csv_path/archive/$(date +"%Y-%m-%d")/
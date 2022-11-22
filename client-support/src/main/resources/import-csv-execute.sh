cd $IOTDB_HOME/tools
./import-csv.sh -h 172.20.70.44 -p 6667 -u root -pw root -f ~/import-csv-aligned.csv -aligned true
./import-csv.sh -h 172.20.70.44 -p 6667 -u root -pw root -f ~/import-csv-nonaligned.csv

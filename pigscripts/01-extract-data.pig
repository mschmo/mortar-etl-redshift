/**
  *  Step 1 - Extract all of your data from various sources and store into a staging location in S3.
  *
  *  See http://help.mortardata.com/data_apps/redshift_data_warehouse/extract_your_data for help.
  */

data = <Your Load Statement Goes Here>

rmf $OUTPUT_PATH/extract;
store data into '$OUTPUT_PATH/extract' using PigStorage();

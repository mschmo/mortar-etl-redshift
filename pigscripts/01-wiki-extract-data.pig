/**
  *  Step 1 - Extract all of your data from various sources and store into a staging location in S3.
  */

%default OUTPUT_PATH 's3://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/wikipedia'

raw = load '$INPUT_PATH'
     using PigStorage(' ')
        as (
            wiki_code:chararray,
            article:chararray,
            monthly_pageviews:int,
            encoded_hourly_pageviews:chararray
        );

data = foreach raw generate wiki_code, article, encoded_hourly_pageviews;

rmf $OUTPUT_PATH/extract;
store data into '$OUTPUT_PATH/extract' using PigStorage();

/**
  *  Step 2 - Transform your data and prepare it for loading into Redshift.
  */

REGISTER '../udfs/jython/wikipedia.py' USING jython AS wikipedia;

%default OUTPUT_PATH 's3://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/wikipedia'

-- Load data from the extract step.
raw =  load '$OUTPUT_PATH/extract'
      using PigStorage()
         as (
              wiki_code:chararray,
              article:chararray,
              encoded_hourly_pageviews:chararray
          );

-- use jython UDFs to decode wiki_code and encoded_hourly_pageviews
decoded =  foreach raw
          generate wiki_code,
                   flatten(wikipedia.decode_wikicode(wiki_code))
                       as (language, wiki_type),
                   article,
                   flatten(wikipedia.decode_pageviews(encoded_hourly_pageviews))
                       as (day, hour, pageviews);

/*
 *  In order to improve parallelization when loading our data into Redshift we're
 *  using Hadoop to store the data into multiple output files.   The Mortar ETL pipeline
 *  automatically calculates the right parallelization value to use for your cluster size.
 * 
 *  It's important that you group on a field(s) that will result in an a number of groups a couple of
 *  times greater than your cluster size and with each group having an approximately equal split of
 *  your data.  In this example, we group by the (day, hour) combination.  If we grouped on 
 *  a field like langauge we would have only a few groups and a very skewed division of data 
 *  as there are languages with many orders of magnitude more data than others.
 *   
 *  We're also going to ensure that the output files are compressed to improve performance
 *  when loading into Redshift.
 */

grouped_data =    group decoded 
                     by (day, hour) 
               parallel $REDSHIFT_PARALLELIZATION;
reduced =  foreach grouped_data 
          generate flatten(decoded);
 
-- Use gzip compression
set output.compression.enabled true;
set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;

-- remove any existing data
rmf $OUTPUT_PATH/transform;
store reduced into '$OUTPUT_PATH/transform' using PigStorage('\t');

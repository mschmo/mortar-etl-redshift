import luigi
from luigi import configuration
from luigi.contrib import redshift
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import dbms
from mortar.luigi import mortartask

import logging
logger = logging.getLogger('luigi-interface')

"""
This luigi pipeline builds an Amazon Redshift data warehouse from Wikipedia 
page view data stored in MySQL.

Instructions to Use:

1. Install the example wiki table into your MySQL database. You can download SQL
   statements to create and populate the table from 
   https://s3.amazonaws.com/mortar-example-data/wikipedia-mysql/mysql-wiki-data.tar.gz

2. Ensure that you have setup your secure project configuration variables:

    # Target Redshift database
    mortar config:set HOST=<my-endpoint.redshift.amazonaws.com>
    mortar config:set PORT=5439
    mortar config:set DATABASE=<my-database-name>
    mortar config:set USERNAME=<my-master-username>
    mortar config:set PASSWORD=<my-master-username-password>

    # Source MySQL database
    mortar config:set MYSQL_DBNAME=<my-mysql-database-name>
    mortar config:set MYSQL_HOST=<my-mysql-host-name>
    mortar config:set MYSQL_USER=<my-mysql-username>
    mortar config:set MYSQL_PASSWORD=<my-mysql-password>

3. Move the client.cfg.template with additional MySQL configuration items
   into place:

    cp luigiscripts/mysql.client.cfg.template luigiscripts/client.cfg.template

TaskOrder:
    ExtractFromMySQL
    TransformWikipediaDataTask
    CopyToRedshiftTask
    ShutdownClusters

To run the pipeline:

    mortar luigi luigiscripts/wikipedia-luigi-mysql.py \
        --output-base-path "s3://<your-bucket-name>/wiki" \
        --table-name "pageviews"
"""

def create_full_path(base_path, sub_path):
    """
    Helper function for constructing paths.
    """
    return '%s/%s' % (base_path, sub_path)

class WikipediaETLPigscriptTask(mortartask.MortarProjectPigscriptTask):
    """
    This is the base class for all of our Mortar related Luigi Tasks.  It extends
    the generic MortarProjectPigscriptTask to set common defaults we'll use
    for this pipeline: common data paths, default cluster size, and our Mortar project name.
    """

    # The base path to where output data will be written.  This will be an S3 path.
    output_base_path = luigi.Parameter()

    # The cluster size to use for running Mortar jobs.  A cluster size of 0
    # will run in Mortar's local mode.  This is a fast (and free!) way to run jobs
    # on small data samples.  Cluster sizes >= 2 will run on a Hadoop cluster.
    cluster_size = luigi.IntParameter(default=5)

    def token_path(self):
        """
        Luigi manages dependencies between tasks by checking for the existence of
        files.  When one task finishes it writes out a 'token' file that will
        trigger the next task in the dependency graph.  This is the base path for
        where those tokens will be written.
        """
        return self.output_base_path

    def default_parallel(self):
        """
        This is used for an optimization that tells Hadoop how many reduce tasks should be used
        for a Hadoop job.  By default we'll tell Hadoop to use the number of reduce slots
        in the cluster.
        """
        if self.cluster_size - 1 > 0:
            return (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE
        else:
            return 1

    def number_of_files(self):
        """
        This is used for an optimization when loading Redshift.  We can load Redshift faster by
        splitting the data to be loaded across multiple files.
        """
        if self.cluster_size - 1 > 0:
            return 2 * (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE
        else:
            return 2

class TransformWikipediaDataTask(WikipediaETLPigscriptTask):
    """
    This task runs the data transformation script pigscripts/02-wiki-transform-data.pig.
    """

    # Table name where wiki data is stored in MySQL
    mysql_table_name = luigi.Parameter(default='wiki')

    def requires(self):
        """
        Tell Luigi to run the MySQL data extraction before this task.
        """
        extract_output_path = create_full_path(self.output_base_path, 'extract')
        return [
            dbms.ExtractFromMySQL(
                table=self.mysql_table_name,
                columns='wiki_code, article, encoded_hourly_pageviews',
                output_path=extract_output_path,
                raw=True)
        ]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'transform'))]

    def parameters(self):
        return { 'OUTPUT_PATH': self.output_base_path,
                 'REDSHIFT_PARALLELIZATION': self.number_of_files()
                }

    def script(self):
        return '02-wiki-transform-data.pig'


class CopyToRedshiftTask(redshift.S3CopyToTable):
    """
    This task copies data from S3 to Redshift.
    """

    # This is the Redshift table where the data will be written.
    table_name = luigi.Parameter()

    # As this task is writing to a Redshift table and not generating any output data
    # files, this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # The schema of the Redshift table where the data will be written.
    columns =[
        ('wiki_code', 'text'),
        ('language', 'text'),
        ('wiki_type', 'text'),
        ('article', 'varchar(max)'),
        ('day', 'int'),
        ('hour', 'int'),
        ('pageviews', 'int'),
        ('PRIMARY KEY', '(article, day, hour)')]

    def requires(self):
        """
        Tell Luigi to run the TransformWikipediaDataTask task before this task.
        """
        return [TransformWikipediaDataTask(output_base_path=self.output_base_path)]

    def redshift_credentials(self):
        """
        Returns a dictionary with the necessary fields for connecting to Redshift.
        """
        config = configuration.get_config()
        section = 'redshift'
        return {
            'host' : config.get(section, 'host'),
            'port' : config.get(section, 'port'),
            'database' : config.get(section, 'database'),
            'username' : config.get(section, 'username'),
            'password' : config.get(section, 'password'),
            'aws_access_key_id' : config.get(section, 'aws_access_key_id'),
            'aws_secret_access_key' : config.get(section, 'aws_secret_access_key')
        }

    def transform_path(self):
        """
        Helper function that returns the root directory where the transformed output
        has been stored.  This is the data that will be copied to Redshift.
        """
        return create_full_path(self.output_base_path, 'transform')

    def s3_load_path(self):
        """
        We want to load all files that begin with 'part' (the hadoop output file prefix) that
        came from the output of the transform step.
        """
        return create_full_path(self.transform_path(), 'part')

    """
    Property methods for connecting to Redshift.
    """

    @property
    def aws_access_key_id(self):
        return self.redshift_credentials()['aws_access_key_id']

    @property
    def aws_secret_access_key(self):
        return self.redshift_credentials()['aws_secret_access_key']

    @property
    def database(self):
        return self.redshift_credentials()['database']

    @property
    def user(self):
        return self.redshift_credentials()['username']

    @property
    def password(self):
        return self.redshift_credentials()['password']

    @property
    def host(self):
        return self.redshift_credentials()['host'] + ':' + self.redshift_credentials()['port']

    @property
    def table(self):
        return self.table_name

    @property
    def copy_options(self):
        return 'GZIP'


class ShutdownClusters(mortartask.MortarClusterShutdownTask):
    """
    This is the very last task in the pipeline.  It will shut down all active
    clusters that are not currently running jobs.
    """

    # These parameters are not used by this task, but passed through for earlier tasks to use.
    # Redshift table name
    table_name = luigi.Parameter()

    # As this task is only shutting down clusters and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that the CopyToRedshiftTask task needs to be completed
        before running this task.
        """
        return [CopyToRedshiftTask(output_base_path=self.output_base_path,
                                   table_name=self.table_name)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]


if __name__ == "__main__":
    """
    We tell Luigi to run the last task in the task dependency graph.  Luigi will then
    work backwards to find any tasks with its requirements met and start from there.

    The first time this pipeline is run the only task with its requirements met will be
    ExtractWikipediaDataTask which does not have any dependencies.
    """
    luigi.run(main_task_cls=ShutdownClusters)

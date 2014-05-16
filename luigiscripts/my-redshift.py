import luigi
from luigi import configuration
from luigi.contrib import redshift
from mortar.luigi import mortartask
from luigi.s3 import S3Target, S3PathTask

"""
This luigi pipeline builds an Amazon Redshift data warehouse.

To run, setup client.cfg with your Mortar username and API key, your s3 keys, and your Redshift cluster
information.

TaskOrder:
    ExtractDataTask
    TransformDataTask
    CopyToRedshiftTask
    ShutdownClusters

To run:
    mortar local:luigi luigiscripts/my-redshift.py
        -p output-base-path=s3://mortar-example-output-data/<your-user-name>/my-redshift
        -p input-base-path=<Your input data path>
        -p table-name=<Your Redshift table name>
"""

# helper function
def create_full_path(base_path, sub_path):
    return '%s/%s' % (base_path, sub_path)


# REPLACE WITH YOUR PROJECT NAME
MORTAR_PROJECT = '<Your Project Name>'

# REPLACE WITH YOUR REDSHIFT COLUMNS
REDSHIFT_COLUMNS = [
        ('language_code', 'text'),
        ('wiki_type', 'text'),
        ('article_title', 'varchar(max)'),
        ('day', 'int'),
        ('hour', 'int'),
        ('pageviews', 'int'),
        ('PRIMARY KEY', '(article, day, hour)')]


class ETLPigscriptTask(mortartask.MortarProjectPigscriptTask):
    """
    Base class for Pigscript tasks in the ETL pipeline.
    """
    # s3 path to the folder where the input data is located
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # cluster size to use
    cluster_size = luigi.IntParameter(default=2)

    def project(self):
        """
        Name of the mortar project to run.
        """
        return MORTAR_PROJECT

    def token_path(self):
        return self.output_base_path

    def default_parallel(self):
        return (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE

    def number_of_files(self):
        return  2 *  (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE


class ExtractDataTask(ETLPigscriptTask):
    """
    Task that runs the data extraction script pigscripts/01-extract-data.pig.
    """

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'extract'))]

    def parameters(self):
        return { 'OUTPUT_PATH': self.output_base_path,
                 'INPUT_PATH': self.input_base_path,
                }

    def script(self):
        return '01-extract-data.pig'


class TransformDataTask(ETLPigscriptTask):
    """
    Task that runs the data transformation script pigscripts/02-transform-data.pig.
    """

    def number_of_files(self):
        """
        Figure out how many files to split the output into.  Will make loading data
        into Redshift easier.
        """
        return  2 * (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'transform'))]

    def parameters(self):
        return { 'OUTPUT_PATH': self.output_base_path,
                 'INPUT_PATH': self.input_base_path,
                 'REDSHIFT_PARALLELIZATION': self.number_of_files()
                }

    def script(self):
        return '02-transform-data.pig'

    def requires(self):
        return ExtractDataTask(
                        self.cluster_size,
                        input_base_path=self.input_base_path,
                        output_base_path=self.output_base_path)


class CopyToRedshiftTask(redshift.S3CopyToTable):
    """
    Copy data from S3 to Redshift

    Parameters:
        path=s3://<path to where your copy files exist>
        manifestpath=s3://<path to manifest_file>
        table_name = <table you wish to copy to>
    """
    table_name = luigi.Parameter()
    output_base_path = luigi.Parameter()

    # not used but required for dependent tasks
    input_base_path = luigi.Parameter()

    columns = REDSHIFT_COLUMNS

    def requires(self):
        return TransformDataTask(
                        cluster_size=5,
                        input_base_path=self.input_base_path,
                        output_base_path=self.output_base_path)

    def redshift_credentials(self):
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
        return create_full_path(self.output_base_path, 'transform')

    def s3_load_path(self):
        """
        We want to load all files that begin with 'part' (the hadoop output file prefix) that
        came from the output of the transform step.
        """
        return create_full_path(self.transform_path(), 'part')

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
    When the pipeline is completed, shut down all active clusters not currently running jobs
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()
    table_name = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    def requires(self):
        return [CopyToRedshiftTask(input_base_path=self.input_base_path, output_base_path=self.output_base_path, table_name=self.table_name)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]


if __name__ == "__main__":
    luigi.run(main_task_cls=ShutdownClusters)

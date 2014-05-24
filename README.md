# Mortar ETL Pipeline for Redshift

A customizable ETL pipeline for building an Amazon Redshift data warehouse by [Mortar Data](http://www.mortardata.com).

## Getting Started / Tutorials

This project contains a complete, runnable example of the Mortar ETL pipeline on example data, as well as a template project for easily getting started with your own data.

For a complete tutorial and explanation of how the ETL pipeline works, see the [Build an Amazon Redshift Data Warehouse tutorial](http://help.mortardata.com/data_apps/redshift_data_warehouse).

## Asking Questions

If you have any questions about this project, please post to the [Mortar Q&A Forum](https://answers.mortardata.com/) to ask Mortar's engineers and data scientists.

## Common Problems

The mortar-etl-redshift project has a dependency on the PostgresSQL Python library [Psycopg2](http://initd.org/psycopg/).  This library requires your system to be able to compile C Python extensions against the libpq library.  If your system is not set up for that you will see an error like:

    ...
    Installing user defined python dependencies... failed
     !
     !    Unable to setup a python environment with your dependencies, see dependency_install.log for more details

To fix this error see, follow the steps at [Psyopg Library](http://help.mortardata.com/data_apps/redshift_data_warehouse/setup_your_project#toc_2PsycopgLibrary).


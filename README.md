![MambaETL.png](_markdown%2FMambaETL.png)

# openmrs-module-mamba-etl

## **Background**

MambaETL (or simply known as Mamba) is an OpenMRS (Open Electronic Medical Records System) implementation for data Extraction, Loading and Transforming (ETL).
From highly normalised data such as the Observeration (Obs) data into a more denormalised format for faster data retrieval and analysis.

OpenMRS stores patient observational data in a long format. Essentially, for each encounter type for a given patient, multiple rows are saved into the OpenMRS Obs table. Sometimes as many as 50 or more rows saved for a single encounter in just the Obs table.

This means that the Obs table quickly grows to millions of records in fairly sized facilities making reporting and any analysis on such data incredibly difficult and slow.

## **Purpose of this module**

The `openmrs-module-mamba-etl` module found [here](https://github.com/UCSF-IGHS/openmrs-module-mamba-core) or simply the MambaETL module is an OpenMRS example module of an ETL implementation. The module can also be used as is or alternatively an implementer can
can use it as a starter module to build on a more robust ETL solution. 

Out of the box, this module is a collection of familiar artefacts/tooling that collectively offer out-of-the box database flattening/transposing and abstraction of repetitive reporting tasks 
so that implementers, analysts, data scientists or teams building reports focus on building without worrying about system performance bottlenecks or bothering too much about how the data is extracted from the primary data source into the reporting destination.

This module specifically has a dependency on the MambaETL core library `openmrs-module-mamba-core` found [here](https://github.com/UCSF-IGHS/openmrs-module-mamba-core/releases).

## **A Quick start-up setup guide**

<span style='color: red;'>Pre-Requisites</span>

* A running instance of the OpenMRS server
* The OpenMRS database user used by the running OpenMRs server must have elevated rights to be able to create databases (i.e. the analysis (ETL) db), Stored Procedures and Functions.
* A second database user with full access to the ETL database to drop and create Tables (should be configured in the openmrs-runtime-properties, more on this later)
* MySQL v5.6 and above running
* Java functionality (JAVA 7 & above)
* Access to the OpenMRS admin interface to be able to re-configure the ETL task

<span style='color: red;'>How to setup and run the module</span>

MambaETL table flattening comes bundled with MambaETL out of the box.

However for this to work a few steps need to be taken:

1. Check the OpenMRS database user priviledges and confirm that the user has the privilidges to create new databases, stored procudures and Functions in this database.
   
   Should the user not have enough priviledges, run the command to grant them.
   
    `-- Check to see the privileges user has` 

   `SHOW GRANTS FOR 'openmrs_user'@'localhost';`

   `-- Grant privileges to create databases` 

    `GRANT CREATE DATABASE ON *.* TO 'openmrs_user'@'localhost';`

   `-- Grant privileges to create stored procedures and functions`

   `GRANT CREATE ROUTINE ON *.* TO 'openmrs_user'@'localhost';`

    `-- update the priviledges`

    `FLUSH PRIVILEGES';` 
   
   This user is needed with the right priviledges because once the MambaETL module has been deployed, at starts up, there is a liquibase changeset that needs to run and do two things:
   -  Create the specified ETL database (`analysis_db` by default)   
   -  Create a number of MambaETL stored procedures and Functions in the `analysis-db`
   ![routines.png](_markdown%2Froutines.png)


2. Add a database user configurations to the OpenMRS runtime properties file.
   
   Create a separate database user with enough priviledges to the analysis_db (ETL) database (Or use the same user as above).
   
   The user should be able to create and drop Tables in the analysis_db (ETL) database. 

   For-example given the openmrs properties file below:
   
        cat /Users/smallgod/openmrs/mambaetl-ref-app/openmrs-runtime.properties 

   Add the following database user configurations to that file

        mambaetl.analysis.db.driver=com.mysql.cj.jdbc.Driver

        mambaetl.analysis.db.url=jdbc\:mysql\://localhost\:3306/analysis_db?useSSL\=false&autoReconnect\=true

        mambaetl.analysis.db.username=mamba

        mambaetl.analysis.db.password=iopdRmgaphk
   

2. Upload the MambaETL module to your OpenMRS instance


3. Go to OpenMRs admin interface and configure as desired the scheduler options for the running of the ETL. 

    
    MambaETL can be scheduled to run automatically every 12 hours after deploying the scripts.
Schedulable Class name: 
`org.openmrs.module.mambacore.task.FlattenTableTask`
![Scheduler.png](_markdown%2FScheduler.png)
But you can adjust the timing for executing MambaETL in the openmrs scheduler if need be as shown below
Under Administration go to scheduler and then click on manage scheduler
![Modify Scheduler.png](_markdown%2FModify%20Scheduler.png)
Click on Schedule and then modify the timings.
![Schedule time.png](_markdown%2FSchedule%20time.png)

4. After the module has been deployed successfuly and the configured scheduler has run, MambaETL related tables and flat tables will be automatically dropped (if exist) and re-created:
![tables.png](_markdown%2Ftables.png)

   
## **Known issues/Limitations**
1. Scheduler sometimes may not be automatically created. You may need to go to where task schedulers page and create one appropriately. 


2. Tables with columns bigger than 160 may fail to insert due to a MYSQL size constraint

## **Frequently Asked Questions (FAQs)**

## **Other features**
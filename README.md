# openmrs-module-mamba-etl

![MambaETL.png](..%2F..%2F..%2F..%2F_markdown%2FMambaETL.png)

## **Background**

MambaETL (or simply known as Mamba) is an OpenMRS (Open Electronic Medical Records System) implementation of data Extraction, Loading and Transforming (ETL) of data into a more denormalised format for faster data retrieval and analysis.

OpenMRS stores patient observational data in a long format. Essentially, for each encounter type for a given patient, multiple rows are saved into the OpenMRS Obs table. Sometimes as many as 50 or more rows saved for a single encounter in just the Obs table.

This means that the Obs table quickly grows to millions of records in fairly sized facilities making reporting and any analysis on such data incredibly slow and difficult.

## **Purpose of this module**

The `openmrs-module-mamba-etl` module is a demo or example module showcasing how to use the MambaETL core library `openmrs-module-mamba-core` to achieve automated flattening of OpenMRS data.
The module is found [here](https://github.com/UCSF-IGHS/openmrs-module-mamba-core). When this module is deployed as is, out of the box it supports flattening of Encounters and Obs data in OpenMRS.

It uses [v1.0.0](https://github.com/UCSF-IGHS/openmrs-module-mamba-core/releases) of MambaETL Core.

You can read more about MambaETL [here](https://ucsf-ighs.notion.site/MambaETL-Documentation-v1-0-3f0467b435744e34a261049383c5e4ef?pvs=4).
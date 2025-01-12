# Spotify-AWS-ETL-Project
In this project, I designed and implemented an ETL (Extract, Transform, Load) pipeline leveraging the Spotify API and AWS services. The pipeline extracts data from the "Spotify Top 100: Global Songs" playlist, transforms it into a structured and analysis-ready format, and stores in the AWS cloud for further processing and insights generation.

## Extract:
- Developed an AWS Lambda function using Python to ingest data from the Spotify API.
- Integrated the spotipy library as a Lambda layer to facilitate seamless interaction with the Spotify API.
- Configured an AWS CloudWatch trigger to automate data ingestion at 5-minute intervals and stor the data into S3 bucket under _raw_data/to_process_

## Transform:
- Designed and implemented an AWS Lambda function to perform data transformation.
- Configured an S3 trigger to automate the process of extracting raw data from the _raw/to_process_ directory, applying transformations, and moving the processed data to the _raw_data/processed_ directory.
- Organized the transformed data into separate folders - Albums, Songs, and Artists within the S3 bucket for structured storage and ease of access.

## Load:
- Utilized AWS Glue Crawler to infer the schema from the transformed data and store the metadata in the AWS Glue Data Catalog.
- Created a database and corresponding tables in the Glue Data Catalog for Albums, Songs, and Artists to organize the transformed data.
- Leveraged AWS Athena to perform analytical queries on the final dataset, enabling insights generation.

# Summary of AWS Serviecs Used:
- AWS Lambda: A serverless compute service that runs code in response to events, without provisioning or managing servers.
- AWS CloudWatch: A monitoring and observability service that collects and visualizes logs, metrics, and events for AWS resources and applications.
- AWS Glue Crawler: A service that automatically scans data sources, infers schemas, and populates the AWS Glue Data Catalog.
- AWS Glue Data Catalog: A centralized metadata repository that stores information about data sources, schemas, and transformations.
- AWS Athena: An interactive query service that allows SQL-based analysis of data stored in Amazon S3.

# Architecture of the ETL Pipeline

![Spotify AWS ETP Pipeline](https://github.com/user-attachments/assets/149edc6a-5ef3-4ff0-be5a-a6a7698d5f92)

ETL Logic for sample data using Spark and AWS resources

An ETL logic is written in Spark for transforming the given data
set present in S3, and query on the transformed data is run using AWS
Redshift. The data sets are in json format. All the raw data in json
format has to be first uploaded to an S3 source bucket. Using EMR, a Spark job
is executed, which would fetch the source data from S3 source
bucket, and then perform the necessary transformations on it as per
the problem statement. Finally, store the transformed data were to
partitioned and stored in parquet format in S3 destination bucket.
Now, these files are accessed using AWS Redshift by running SQL queries
on the transformed processed data. 

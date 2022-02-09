from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window

  
################ Creating SparkSession #########################
spark = SparkSession.builder.appName('capstone').getOrCreate()

################ LOADING INPUT DATA INTO DATAFRAMES FROM S3 #######################
memberships_df = spark.read.format('json').load('s3://capstone-project-input/input/memberships.json')
organizations_df = spark.read.format('json').load('s3://capstone-project-input/input/organizations.json')
persons_df = spark.read.format('json').load('s3://capstone-project-input/input/persons.json')

################ Performing inner join on persons and memberships df, result stored in stage1_df #############
stage1_df = persons_df.join(memberships_df,persons_df.id == memberships_df.person_id,"inner")

################ Renaming columns in organizations df ##################
col_list = ['classification','org_id','org_identifiers','org_image','org_links','org_name','org_othernames','seats','type']
organizations_df = organizations_df.toDF(*col_list)

################ Performing inner join on stage1_df and organizations df, result stored in stage2_df ##########
stage2_df = stage1_df.join(organizations_df,organizations_df.org_id==stage1_df.organization_id,"inner")

################ Dropping redundant columns ##############################
stage3_df = stage2_df.drop('person_id','org_id','identifiers','images','links','other_names')

################ Adding additional columns for indexing ######################
############### Adding an additional columns in stage3_df, for indexing, i.e., row numbering #########

windowPartition = Window().orderBy(lit(1))
stage3_df = stage3_df.withColumn('cid',row_number().over(windowPartition))
stage3_df = stage3_df.withColumn('cdid',row_number().over(windowPartition))


################ Splitting stage3 table into 2 tables : stage3_df and stage3_cd_df, where stage3_cd_df contains only contact details information ######
stage3_cd_df = stage3_df.select(explode('contact_details'),'cdid')
stage3_df = stage3_df.drop('cdid')

################ Flattening array in stage3_cd_df ###############
stage3_cd_df=stage3_cd_df.select('col.*','cdid')

################ Renaming the flattened columns #################
stage3_cd_df=stage3_cd_df.withColumnRenamed('type','contact_details_type')
stage3_cd_df=stage3_cd_df.withColumnRenamed('value','contact_details_value')


############### Dropping redundant columns in stage3_df ###########################
stage3_df = stage3_df.drop('contact_details','org_identifiers','org_links','org_othernames')

############### MAIN TABLE - stage3_df     ,     SECOND TABLE - stage3_cd_df ########################
############### Performing inner join on stage3_df and stage3_cd_df, based on the id columns that we just created, result stored in stage4_df ###########
stage4_df = stage3_df.join(stage3_cd_df, stage3_df.cid == stage3_cd_df.cdid, 'inner')

############### Finally writing the joined output, i.e., stage4_df as parquet file, after partitioning by 'type' column, into S3 ###########
stage4_df.write.partitionBy("contact_details_type").mode("overwrite").parquet("s3://capstone-project-input/output/output-files")
'''
+--------------------+-----+
|contact_details_type|count|
+--------------------+-----+
|                 fax| 3607|
|               phone| 3723|
|             twitter| 3109|
+--------------------+-----+
'''


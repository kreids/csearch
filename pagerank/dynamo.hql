CREATE EXTERNAL TABLE IF NOT EXISTS s3_import(a_col string, b_col float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 's3://bucketname/path/subpath/';                    

CREATE EXTERNAL TABLE IF NOT EXISTS hiveTableName (col1 string, col2 float)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
TBLPROPERTIES ("dynamodb.table.name" = "pagerank-results", 
"dynamodb.column.mapping" = "col1:url,col2:rank");  

INSERT OVERWRITE TABLE hiveTableName SELECT * FROM s3_import;

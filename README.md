To get an estimated partition # to be used in Spark for compaction based on the Input sizes of data in S3

Set the values for the below variables in the script
```
    l_s3_bucket = "your-bucket-name"
    l_s3_prefix = "sample/prefix/in/s3/"
    l_input_format = "TEXT"  # TEXT / PARQUET / AVRO
    l_input_compression = "GZIP"  # NONE / SNAPPY / LZO / GZIP / BZ2
    l_output_format = "PARQUET"  # TEXT / PARQUET / AVRO
    l_output_compression = "GZIP"  # NONE / SNAPPY / LZO / GZIP / BZ2
    
```

The target file size for compaction is between 32 MB to 128 MB
There are several factors involved that affects the compression ratio in Parquet format so the estimations are extremely rough.

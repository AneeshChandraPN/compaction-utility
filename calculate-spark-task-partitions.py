import boto3


def get_size(bucket, path):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    total_size = 0

    for obj in my_bucket.objects.filter(Prefix=path):
        total_size = total_size + obj.size

    return total_size


def get_format_ratio(format):
    format_ratio = 1
    if format == "PARQUET":
        format_ratio = 2
    elif format == "AVRO":
        format_ratio = 1.6

    return format_ratio


def get_compression_ratio(compression):
    compression_ratio = 1
    if compression == "SNAPPY":
        compression_ratio = 1.7
    elif compression == "LZO":
        compression_ratio = 2
    elif compression == "GZIP":
        compression_ratio = 2.5
    elif compression == "BZ2":
        compression_ratio = 3.33

    return compression_ratio


def get_fit_partitions(input_size, input_format, input_compression,
                       output_format=None, output_compression=None):

    l_input_format_ratio = get_format_ratio(input_format)
    l_input_compression_ratio = get_compression_ratio(input_compression)

    l_output_format_ratio = get_format_ratio(output_format)
    l_output_compression_ratio = get_compression_ratio(output_compression)
    uncompressed_input_size = input_size * l_input_format_ratio * l_input_compression_ratio
    compressed_output_size = uncompressed_input_size / l_output_format_ratio / l_output_compression_ratio
    return round(compressed_output_size / 1024 / 1024 / 64)


if __name__ == '__main__':
    l_s3_bucket = "your-bucket-name"
    l_s3_prefix = "sample/prefix/in/s3/"
    l_input_format = "TEXT"  # TEXT / PARQUET / AVRO
    l_input_compression = "NONE"  # NONE / SNAPPY / LZO / GZIP / BZ2
    l_output_format = "PARQUET"  # TEXT / PARQUET / AVRO
    l_output_compression = "GZIP"  # NONE / SNAPPY / LZO / GZIP / BZ2

    l_input_size = get_size(l_s3_bucket, l_s3_prefix)
    l_total_partitions = get_fit_partitions(l_input_size, l_input_format, l_input_compression,
                                            l_output_format, l_output_compression)

    print("Total input size of the data is {} MB".format(str(round(l_input_size / 1024 / 1024))))
    print("To achieve an optimal target file size based on the below parameters")
    print("--------------------------------------------------------------------")
    print("INPUT FILE FORMAT = {}".format(l_input_format))
    print("INPUT FILE COMPRESSION = {}".format(l_input_compression))
    print("OUTPUT FILE FORMAT = {}".format(l_output_format))
    print("OUTPUT FILE COMPRESSION = {}".format(l_output_compression))
    print("--------------------------------------------------------------------")
    print("Total # of partitions for compaction = {}".format(str(l_total_partitions)))
    print("--------------------------------------------------------------------")
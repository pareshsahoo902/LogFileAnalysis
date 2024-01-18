from pyspark.sql import *
from pyspark.sql.functions import *

from utils import process_spark_df, write_spark_df_to_oracle

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Hello Spark").master("local[3]").config("spark.driver.extraClassPath",
                                                                                  "C:\spark\jars\ojdbc8.jar").config(
        "spark.executor.extraClassPath", "C:\spark\jars\ojdbc8.jar").config("spark.sql.debug.maxToStringFields",
                                                                            100).getOrCreate()

    file_df = spark.read.text("data/prod_logs.txt")

    pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (\w+)\s+\[(.*?)\]\s+(\S+?) - (.*)$'
    log_df = file_df.select(
        regexp_extract('value', pattern, 1).alias('timestamp'),
        regexp_extract('value', pattern, 2).alias('log_level'),
        regexp_extract('value', pattern, 3).alias('class_name'),
        regexp_extract('value', pattern, 4).alias('pentaho_step'),
        regexp_extract('value', pattern, 5).alias('log_message'),
    )
    log_df = log_df.where('trim(pentaho_step)!= ""')

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.utils import AnalysisException

    regex_pattern = r'\b[A-Z]+\d+[A-Za-z]*Main\b'
    source_column_name = "pentaho_step"
    target_column_name = "connection_name"

    # Call the function to process the DataFrame
    log_df = process_spark_df(log_df, regex_pattern, source_column_name, target_column_name)

    grouped_data = log_df.drop("class_name").sort("timestamp")

    table_name = "pentaho_log_aswini"
    username = "IQBIZ"
    password = "TiberADW1#2#"
    dsn = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=f1icmgkywmxvdbu_adwdev_medium.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"

    write_spark_df_to_oracle(grouped_data, table_name, username, password, dsn, mode="replace")

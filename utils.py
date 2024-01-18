from pyspark.errors import AnalysisException
from sqlalchemy import create_engine
from pyspark.sql.functions import *


def process_spark_df(df, regex, source_col, target_col):
    """
    Process a PySpark DataFrame by renaming and filtering based on the given parameters.

    Parameters:
    - df: PySpark DataFrame
    - regex: Regular expression for filtering
    - source_col: Source column name to be renamed
    - target_col: Target column name after renaming

    Returns:
    - Processed PySpark DataFrame
    """
    try:
        # Renaming the column
        processed_df = df.withColumnRenamed(source_col, target_col)

        # Filtering based on the provided regex
        processed_df = processed_df.filter(col(source_col).rlike(regex))

        return processed_df

    except AnalysisException as e:
        print(f"An error occurred: {e}")
        # Handle or log the exception as needed
        return None


def write_spark_df_to_oracle(grouped_data, table_name, username, password, dsn, mode="replace"):
    """
    Write a PySpark DataFrame to an Oracle database table.

    Parameters:
    - grouped_data: PySpark DataFrame
    - table_name: Name of the target Oracle table
    - username: Oracle database username
    - password: Oracle database password
    - dsn: Oracle database connection details
    - mode: Write mode ("replace", "append", or "ignore")

    Returns:
    None
    """
    # Create an Oracle URI for SQLAlchemy
    oracle_uri = f"oracle+cx_oracle://{username}:{password}@{dsn}"

    # Create SQLAlchemy engine
    engine = create_engine(oracle_uri)

    try:
        # Convert PySpark DataFrame to Pandas DataFrame
        data_pd = grouped_data.toPandas()

        # Write Pandas DataFrame to Oracle table
        data_pd.to_sql(name=table_name, con=engine, if_exists=mode, index=False)

    except Exception as e:
        print(f"An error occurred: {e}")
        # Handle or log the exception as needed

    finally:
        # Close the SQLAlchemy connection
        engine.dispose()

    # Specify your regex, source, and target column names

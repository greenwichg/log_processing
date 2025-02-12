# preprocessing.py

from pyspark.sql.functions import col, current_timestamp, to_date

def preprocess_data(df):
    """
    Preprocess the input DataFrame by:
      - Filtering out records with null values in TTI or TTAR.
      - Converting TTI and TTAR from milliseconds to seconds.
      - Adding a processing timestamp.
      - Deriving an event_date column from event_timestamp.
    """
    df_filtered = df.filter(col("TTI").isNotNull() & col("TTAR").isNotNull())
    df_transformed = df_filtered.withColumn("TTI_seconds", col("TTI") / 1000) \
                                .withColumn("TTAR_seconds", col("TTAR") / 1000) \
                                .withColumn("processing_timestamp", current_timestamp()) \
                                .withColumn("event_date", to_date(col("event_timestamp")))
    return df_transformed

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================================
# GOLDEN RECORD ASSEMBLY
# ============================================================================

@dp.temporary_view()
def records_with_golden_names():
    """
    Join clusters with survivor names to assign golden_name to each record.
    """
    clusters = spark.read.table("name_clusters")
    survivors = spark.read.table("survivor_names")
    
    return (
        clusters
        .join(survivors, "cluster_id", "left")
    )


@dp.temporary_view()
def golden_npi_resolution():
    """
    Resolve golden NPI for each golden_name:
    1. Use existing NPI if available
    2. If missing, find NPI from other records with same golden_name
    3. If still missing, flag as "Need Manual Review"
    """
    records = spark.read.table("records_with_golden_names")
    
    # For each golden_name, collect all available NPIs
    npi_lookup = (
        records
        .filter(F.col("NPI").isNotNull())
        .filter(F.col("golden_name") != "Need Manual Review")
        .groupBy("golden_name")
        .agg(F.first("NPI", ignorenulls=True).alias("resolved_npi"))
    )
    
    # Join back to assign golden_NPI
    return (
        records
        .join(npi_lookup, "golden_name", "left")
        .withColumn(
            "golden_NPI",
            F.when(
                (F.col("golden_name") == "Need Manual Review") | 
                (F.col("resolved_npi").isNull()),
                "Need Manual Review"
            )
            .otherwise(F.col("resolved_npi"))
        )
        .drop("resolved_npi")
    )


@dp.temporary_view()
def address_deduplication():
    """
    Deduplicate addresses by golden_NPI:
    Keep the most recent Practice_Address_line_1 based on Last_Updated_Date.
    Handle missing addresses by preserving City and Zip when available.
    """
    records = spark.read.table("golden_npi_resolution")
    
    # Convert Last_Updated_Date to proper date type for comparison
    records_with_date = records.withColumn(
        "Last_Updated_Date_typed",
        F.to_date(F.col("Last_Updated_Date"))
    )
    
    # Window to find most recent address per golden_NPI
    address_window = Window.partitionBy("golden_NPI").orderBy(
        F.col("Last_Updated_Date_typed").desc_nulls_last(),
        F.col("Legacy_System_ID")
    )
    
    # Get the most recent address fields
    address_deduped = (
        records_with_date
        .filter(F.col("golden_NPI") != "Need Manual Review")
        .withColumn("address_rank", F.row_number().over(address_window))
        .filter(F.col("address_rank") == 1)
        .select(
            "golden_NPI",
            F.col("Practice_Address_line_1").alias("Golden_Practice_Address_line_1"),
            F.col("Practice_Address_line_2").alias("Golden_Practice_Address_line_2"),
            F.col("City").alias("Golden_City"),
            F.col("State").alias("Golden_State"),
            F.col("Zip").alias("Golden_Zip")
        )
    )
    
    return address_deduped


@dp.temporary_view()
def phone_deduplication():
    """
    Deduplicate phones by golden_NPI:
    Keep the most recent Phone_std based on Last_Updated_Date.
    """
    records = spark.read.table("golden_npi_resolution")
    
    # Convert Last_Updated_Date to proper date type for comparison
    records_with_date = records.withColumn(
        "Last_Updated_Date_typed",
        F.to_date(F.col("Last_Updated_Date"))
    )
    
    # Window to find most recent phone per golden_NPI
    phone_window = Window.partitionBy("golden_NPI").orderBy(
        F.col("Last_Updated_Date_typed").desc_nulls_last(),
        F.col("Legacy_System_ID")
    )
    
    # Get the most recent phone fields
    phone_deduped = (
        records_with_date
        .filter(F.col("golden_NPI") != "Need Manual Review")
        .withColumn("phone_rank", F.row_number().over(phone_window))
        .filter(F.col("phone_rank") == 1)
        .select(
            "golden_NPI",
            F.col("Phone_std").alias("Golden_Phone_std"),
            F.col("Phone_Ext").alias("Golden_Phone_Ext")
        )
    )
    
    return phone_deduped


@dp.temporary_view()
def complete_golden_records():
    """
    Assemble complete golden records with all fields.
    Join NPI resolution with address and phone deduplication.
    Calculate manual review flag count.
    """
    npi_records = spark.read.table("golden_npi_resolution")
    addresses = spark.read.table("address_deduplication")
    phones = spark.read.table("phone_deduplication")
    
    # Join all golden fields
    complete = (
        npi_records
        .join(addresses, "golden_NPI", "left")
        .join(phones, "golden_NPI", "left")
    )
    
    # Count fields needing manual review
    complete_with_flags = complete.withColumn(
        "cnt_manual_review",
        (
            F.when(F.col("golden_NPI") == "Need Manual Review", 1).otherwise(0) +
            F.when(F.col("golden_name") == "Need Manual Review", 1).otherwise(0) +
            F.when(
                (F.col("Golden_Practice_Address_line_1").isNull()) & 
                (F.col("golden_NPI") != "Need Manual Review"),
                1
            ).otherwise(0) +
            F.when(
                (F.col("Golden_Phone_std").isNull()) & 
                (F.col("golden_NPI") != "Need Manual Review"),
                1
            ).otherwise(0)
        )
    )
    
    # Replace "Need Manual Review" with null in optional fields
    final = complete_with_flags.withColumn(
        "Golden_Practice_Address_line_2",
        F.when(
            F.col("Golden_Practice_Address_line_2") == "Need Manual Review",
            F.lit(None)
        ).otherwise(F.col("Golden_Practice_Address_line_2"))
    ).withColumn(
        "Golden_Phone_Ext",
        F.when(
            F.col("Golden_Phone_Ext") == "Need Manual Review",
            F.lit(None)
        ).otherwise(F.col("Golden_Phone_Ext"))
    )
    
    return final

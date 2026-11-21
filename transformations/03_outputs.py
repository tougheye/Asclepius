from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============================================================================
# FINAL OUTPUT TABLES
# ============================================================================

@dp.materialized_view(
    name="cms_mdm_gold.golden_record.golden_table",
    comment="Golden master records with deduplicated doctor information"
)
def golden_table():
    """
    Final golden master table containing deduplicated doctor records.
    Includes golden name, NPI, address, and phone information.
    Only includes records that do not need manual review.
    """
    complete = spark.read.table("complete_golden_records")
    
    return (
        complete
        .filter(F.col("cnt_manual_review") == 0)
        .select(
            F.col("cluster_id").alias("Golden_ID"),
            F.col("golden_name").alias("Golden_Name"),
            F.col("golden_NPI").alias("Golden_NPI"),
            F.col("Golden_Practice_Address_line_1"),
            F.col("Golden_Practice_Address_line_2"),
            F.col("Golden_City"),
            F.col("Golden_State"),
            F.col("Golden_Zip"),
            F.col("Golden_Phone_std"),
            F.col("Golden_Phone_Ext"),
            F.current_timestamp().alias("Created_Timestamp")
        )
        .distinct()
    )


@dp.materialized_view(
    name="cms_mdm_gold.golden_record.manual_review_queue",
    comment="Records requiring manual review due to missing or ambiguous data"
)
def manual_review_queue():
    """
    Manual review queue containing records that need human intervention.
    Includes original records with flags indicating what needs review.
    """
    complete = spark.read.table("complete_golden_records")
    
    return (
        complete
        .filter(F.col("cnt_manual_review") > 0)
        .select(
            F.col("Legacy_System_ID"),
            F.col("First_Name"),
            F.col("Last_Name"),
            F.col("full_name"),
            F.col("NPI"),
            F.col("Practice_Address_line_1"),
            F.col("City"),
            F.col("State"),
            F.col("Zip"),
            F.col("Phone_std"),
            F.col("cluster_id"),
            F.col("golden_name"),
            F.col("golden_NPI"),
            F.col("cnt_manual_review"),
            # Flag columns to indicate what needs review
            F.when(F.col("golden_name") == "Need Manual Review", True)
             .otherwise(False).alias("Name_Needs_Review"),
            F.when(F.col("golden_NPI") == "Need Manual Review", True)
             .otherwise(False).alias("NPI_Needs_Review"),
            F.when(
                (F.col("Golden_Practice_Address_line_1").isNull()) & 
                (F.col("golden_NPI") != "Need Manual Review"),
                True
            ).otherwise(False).alias("Address_Needs_Review"),
            F.when(
                (F.col("Golden_Phone_std").isNull()) & 
                (F.col("golden_NPI") != "Need Manual Review"),
                True
            ).otherwise(False).alias("Phone_Needs_Review"),
            F.col("Last_Updated_Date"),
            F.current_timestamp().alias("Queue_Timestamp")
        )
    )


@dp.materialized_view(
    name="cms_mdm_gold.golden_record.match_quality_summary",
    comment="Summary statistics and quality metrics for the matching process"
)
def match_quality_summary():
    """
    Quality metrics and statistics for the MDM matching process.
    Provides insights into match confidence, cluster sizes, and review rates.
    """
    complete = spark.read.table("complete_golden_records")
    pairs = spark.read.table("name_pairs")
    
    # Overall statistics
    total_records = complete.count()
    total_clusters = complete.select("cluster_id").distinct().count()
    records_needing_review = complete.filter(F.col("cnt_manual_review") > 0).count()
    
    # Cluster size distribution
    cluster_sizes = (
        complete
        .groupBy("cluster_id")
        .agg(F.count("*").alias("cluster_size"))
    )
    
    # Match confidence distribution
    confidence_dist = (
        pairs
        .groupBy("confidence_level")
        .agg(F.count("*").alias("match_count"))
    )
    
    # Create summary record
    summary = spark.createDataFrame([
        {
            "metric_name": "Total Records",
            "metric_value": total_records,
            "metric_description": "Total number of source records processed"
        },
        {
            "metric_name": "Total Clusters",
            "metric_value": total_clusters,
            "metric_description": "Number of unique golden records (clusters) created"
        },
        {
            "metric_name": "Records Needing Review",
            "metric_value": records_needing_review,
            "metric_description": "Number of records requiring manual review"
        },
        {
            "metric_name": "Review Rate",
            "metric_value": round((records_needing_review / total_records * 100) if total_records > 0 else 0, 2),
            "metric_description": "Percentage of records needing manual review"
        },
        {
            "metric_name": "Deduplication Rate",
            "metric_value": round((1 - total_clusters / total_records) * 100 if total_records > 0 else 0, 2),
            "metric_description": "Percentage reduction in record count after deduplication"
        }
    ])
    
    return summary.withColumn(
        "Generated_Timestamp",
        F.current_timestamp()
    )
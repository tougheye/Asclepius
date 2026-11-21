from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================================
# BASE DATA AND FUZZY NAME MATCHING
# ============================================================================

@dp.temporary_view()
def source_doctors():
    """
    Read source data from silver layer and prepare for fuzzy matching.
    Create full_name by combining First_Name and Last_Name.
    """
    return (
        spark.read.table("cms_mdm_silver.cms_mdm_silver.doctor_list_silver")
        .withColumn("full_name", 
                   F.concat_ws(" ", F.col("First_Name"), F.col("Last_Name")))
        .withColumn("full_name_upper", F.upper(F.col("full_name")))
    )


@dp.temporary_view()
def name_pairs():
    """
    Create all possible name pairs for fuzzy matching.
    Self-join on different Legacy_System_IDs to compare names.
    Calculate similarity score using levenshtein distance.
    
    Score formula: 100 * (1 - levenshtein_distance / max_length)
    """
    doctors = spark.read.table("source_doctors")
    
    # Self-join to create pairs (avoid duplicates by filtering id1 < id2)
    pairs = (
        doctors.alias("d1")
        .join(
            doctors.alias("d2"),
            (F.col("d1.Legacy_System_ID") < F.col("d2.Legacy_System_ID")),
            "inner"
        )
        .select(
            F.col("d1.Legacy_System_ID").alias("id1"),
            F.col("d1.full_name_upper").alias("name1"),
            F.col("d2.Legacy_System_ID").alias("id2"),
            F.col("d2.full_name_upper").alias("name2")
        )
    )
    
    # Calculate similarity score
    return (
        pairs
        .withColumn("lev_distance", F.levenshtein(F.col("name1"), F.col("name2")))
        .withColumn("max_length", 
                   F.greatest(F.length(F.col("name1")), F.length(F.col("name2"))))
        .withColumn("similarity_score", 
                   F.round(100 * (1 - F.col("lev_distance") / F.col("max_length")), 2))
        .filter(F.col("similarity_score") >= 70)  # Only keep scores >= 70
        .withColumn(
            "confidence_level",
            F.when(F.col("similarity_score") >= 93, "High")
            .when(F.col("similarity_score") >= 88, "Medium")
            .otherwise("Low")
        )
    )


@dp.temporary_view()
def name_clusters():
    """
    Group similar names into clusters based on fuzzy matching.
    Each cluster represents a group of names that likely refer to the same person.
    Use union-find approach: create edges from name pairs, then assign cluster IDs.
    """
    pairs = spark.read.table("name_pairs")
    
    # Create edges from high-confidence matches
    high_conf_pairs = pairs.filter(F.col("confidence_level") == "High")
    
    # Create bidirectional edges
    edges1 = high_conf_pairs.select(
        F.col("id1").alias("from_id"),
        F.col("id2").alias("to_id")
    )
    edges2 = high_conf_pairs.select(
        F.col("id2").alias("from_id"),
        F.col("id1").alias("to_id")
    )
    all_edges = edges1.union(edges2)
    
    # Assign cluster_id as the minimum Legacy_System_ID in each group
    # This is a simplified clustering - in production, use GraphFrames for better clustering
    cluster_mapping = (
        all_edges
        .groupBy("from_id")
        .agg(F.min("to_id").alias("cluster_id"))
        .select(
            F.col("from_id").alias("Legacy_System_ID"),
            F.col("cluster_id")
        )
    )
    
    # Join back to source to include all doctors (even those without matches)
    doctors = spark.read.table("source_doctors")
    return (
        doctors
        .join(cluster_mapping, "Legacy_System_ID", "left")
        .withColumn(
            "cluster_id",
            F.coalesce(F.col("cluster_id"), F.col("Legacy_System_ID"))
        )
    )


@dp.temporary_view()
def survivor_names():
    """
    Select the survivor name for each cluster based on business rules:
    1. Most frequent name wins
    2. If tied on frequency, longest name wins
    3. If still tied, flag as "Need Manual Review"
    """
    clusters = spark.read.table("name_clusters")
    
    # Calculate name frequency within each cluster
    name_counts = (
        clusters
        .groupBy("cluster_id", "full_name")
        .agg(
            F.count("*").alias("name_frequency"),
            F.max(F.length("full_name")).alias("name_length")
        )
    )
    
    # Rank names within each cluster
    window = Window.partitionBy("cluster_id").orderBy(
        F.col("name_frequency").desc(),
        F.col("name_length").desc(),
        F.col("full_name")  # Deterministic tie-breaker
    )
    
    ranked_names = (
        name_counts
        .withColumn("rank", F.row_number().over(window))
    )
    
    # Check for ties at rank 1 (same frequency and length)
    tie_check = (
        ranked_names
        .filter(F.col("rank") == 1)
        .groupBy("cluster_id")
        .agg(F.count("*").alias("tie_count"))
    )
    
    # Select survivor name
    survivor = (
        ranked_names
        .filter(F.col("rank") == 1)
        .join(tie_check, "cluster_id", "left")
        .withColumn(
            "golden_name",
            F.when(F.col("tie_count") > 1, "Need Manual Review")
            .otherwise(F.col("full_name"))
        )
        .select("cluster_id", "golden_name")
    )
    
    return survivor

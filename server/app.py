import axelrod as axl
import h3
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, Dict, Set, Tuple, FrozenSet, cast, List
from collections import defaultdict
from functools import lru_cache

# --- PySpark Imports ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, lit, sum as spark_sum, max as spark_max, when as spark_when, coalesce, struct as spark_struct, collect_list, array as spark_array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, LongType

#### Geospatial prisoner's dilemma ####

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

# --- Spark Session Initialization ---
# This should be managed carefully in a production FastAPI app.
# A common pattern is to initialize it once on startup.
spark: Any = None

# In your app.py/main.py
@app.on_event("startup")
async def startup_event():
    """Initializes SparkSession on application startup (for local execution)."""
    global spark
    if spark is None:
        spark = SparkSession.builder \
            .appName("GeospatialPrisonersDilemma") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .master("local[10]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR") # Reduce verbosity
        print("SparkSession initialized successfully for local execution.")

@app.on_event("shutdown")
async def shutdown_event():
    """Stops SparkSession on application shutdown."""
    global spark
    if spark is not None:
        spark.stop()
        print("SparkSession stopped.")

# --- Helper function to get valid neighbors (Pythonic, used inside UDF) ---
# This function is not directly used for Spark DataFrame operations, but its logic
# is embedded within the UDFs where H3 operations are performed.
def get_valid_neighbors_list(center_hex: str, all_hexes: Set[str]) -> List[str]:
    """
    Finds neighbors of center_hex that are also in the all_hexes set.
    Returns as a list for UDF compatibility.
    """
    try:
        potential_neighbors = h3.grid_disk(center_hex, 1)
        actual_neighbors = [neighbor for neighbor in potential_neighbors
                            if neighbor != center_hex and neighbor in all_hexes]
        return actual_neighbors
    except ValueError as e:
        # Log or handle invalid H3 index, or return empty list
        print(f"Warning: Invalid H3 index '{center_hex}' encountered: {e}")
        return []

# --- Cached function to calculate neighbor counts (less relevant with Spark) ---
# With PySpark, you'll compute neighbors directly within the DataFrame operations
# rather than relying on this cached Python function for pre-computation.
# However, the H3 logic can be used within a UDF.
@lru_cache(maxsize=10)
def _get_cached_hex_neighbors_counts(hex_ids_fset: FrozenSet[str]) -> Dict[str, int]:
    """
    Calculates the number of valid neighbors for each hex ID in the input frozenset.
    This function is cached using LRU strategy.
    Args:
        hex_ids_fset: A frozenset of H3 hexagon ID strings.
    Returns:
        A dictionary mapping each hex ID string to its count of valid neighbors.
    Raises:
        ValueError: If any hex_id in hex_ids_fset is invalid for h3.grid_disk.
    """
    hex_ids_set = set(hex_ids_fset)
    counts: Dict[str, int] = {}
    for hex_id in hex_ids_fset:
        try:
            neighbors = get_valid_neighbors_list(hex_id, hex_ids_set)
            counts[hex_id] = len(neighbors)
        except ValueError as e:
            raise ValueError(f"Invalid H3 index '{hex_id}': {e}") from e
    return counts

# --- Strategy Definitions ---
stringToStrat = {
    'Tit-for-tat': axl.TitForTat(),
    'Random': axl.Random(),
    'Harrington': axl.SecondByHarrington(),
    'Tester': axl.SecondByTester(),
    'Defector': axl.Defector(),
    'Cooperator': axl.Cooperator(),
    'Alternator': axl.Alternator(),
    'Suspicious tit-for-tat': axl.SuspiciousTitForTat(),
    'Forgiving tit-for-tat': axl.ForgivingTitForTat(),
    'Grudger': axl.Grudger(),
}

idStrategyType = Dict[int, Tuple[str, str]]

idToStrategy: idStrategyType = {
    0: ("Alternator", "gray"),
    1: ("Cooperator", "green"),
    2: ("Defector", "pink"),
    3: ("Forgiving tit-for-tat", "brown"),
    4: ("Grudger", "orange"),
    5: ("Harrington", "red"),
    6: ("Random", "black"),
    7: ("Suspicious tit-for-tat", "purple"),
    8: ("Tester", "yellow"),
    9: ("Tit-for-tat", "blue"),
}

# --- UDF for Playing a Single Match ---
# This UDF takes the strategy names of two players and returns their scores.
# It's crucial that Axelrod Player objects are re-instantiated within the UDF
# because they are not serializable across Spark workers.
def play_match(strat_name1: str, strat_name2: str, rounds: int, noise: float, r: float, s: float, t: float, p: float) -> Tuple[float, float]:
    game = axl.Game(r=r, s=s, t=t, p=p)
    try:
        player1 = stringToStrat[strat_name1].clone()
        player2 = stringToStrat[strat_name2].clone()
    except KeyError:
        # This case should ideally be caught before calling the UDF if inputs are validated
        return (0.0, 0.0) # Or raise a more specific error that Spark can handle

    match = axl.Match(players=(player1, player2), turns=rounds, noise=noise, game=game)
    try:
        match.play()
        final_scores = match.final_score()
        if final_scores is None:
            return (0.0, 0.0)
        return (float(final_scores[0]), float(final_scores[1]))
    except Exception as e:
        # Log the error but return default scores to avoid breaking the Spark job
        print(f"Error during match between {strat_name1} and {strat_name2}: {e}")
        return (0.0, 0.0)

# Define UDF return type: Array of Float (for the two scores)
play_match_udf = udf(play_match, ArrayType(FloatType()))

# --- UDF for Finding Neighbors and Preparing Match Pairs ---
# This UDF will run on each hex and generate pairs with its neighbors.
# The `all_hexes_broadcast` will be a broadcast variable in Spark.
def get_neighbor_pairs(center_hex_id: str, center_strategy_name: str, all_hex_ids_set_b: Set[str]) -> List[Tuple[str, str, str, str]]:
    """
    Generates (center_hex, neighbor_hex, center_strat, neighbor_strat) tuples.
    This UDF needs access to all hex IDs and their strategies, which means
    broadcasting the hexToStrategy map or joining it.
    For simplicity here, we assume strategies are joined or accessible.
    """
    all_hexes_set = all_hex_ids_set_b # Using the broadcasted set
    neighbors = get_valid_neighbors_list(center_hex_id, all_hexes_set)
    
    # In a real Spark implementation, you'd broadcast hexToStrategy or join
    # to get neighbor strategies. For now, assume a structure that provides it.
    # We'll need the hexToStrategy map available to the UDF. This usually means
    # passing it as a broadcast variable or ensuring the DataFrame structure provides it.
    
    return [] # This UDF is more illustrative of logic, not direct Spark usage

# --- UDF for Determining Next Strategy ---
def determine_next_strategy(
    center_current_strategy: str,
    center_score: float,
    neighbor_data: List[Tuple[str, str, float]] # (neighbor_hex_id, neighbor_strategy_name, neighbor_score)
) -> str:
    """
    Determines the next strategy for the center_hex based on its score and
    its neighbors' scores.
    """
    max_score = center_score
    next_strategy = center_current_strategy

    for neighbor_info in neighbor_data:
        # Check if neighbor_info is not None (in case of outer join with no neighbor)
        if neighbor_info and neighbor_info[2] is not None: 
            neighbor_strat = neighbor_info[1]
            neighbor_score = neighbor_info[2]
            if neighbor_score > max_score:
                max_score = neighbor_score
                next_strategy = neighbor_strat
    return next_strategy

determine_next_strategy_udf = udf(determine_next_strategy, StringType())

neighbor_info_schema = StructType([
    StructField("neighbor_hex_id", StringType(), True),
    StructField("neighbor_strategy_name", StringType(), True),
    StructField("neighbor_score", FloatType(), True)
])

# Define the full ArrayType for neighbor_data
neighbor_data_array_type = ArrayType(neighbor_info_schema)

# --- API Endpoints ---
@app.get("/")
async def root():
    """ Basic root endpoint. """
    return {"message": "Geospatial Prisoner's Dilemma API"}

@app.get("/strategies")
async def strategies() -> idStrategyType:
    """ Returns the mapping of strategy IDs to strategy names and colors. """
    return idToStrategy

@app.post("/game_step")
async def game_step(hexToStrategyID: Dict[str, int], rounds: int = 15, noise: float = 0.0, r: float = 3, s: float = 0, t: float = 5, p: float = 1):
    """
    Simulates one step of the spatial prisoner's dilemma using PySpark.
    """
    global spark # Access the initialized SparkSession

    if spark is None:
        raise HTTPException(status_code=500, detail="SparkSession not initialized.")

    # --- Input Validation ---
    if noise < 0:
        raise HTTPException(status_code=400, detail="Noise must be a non-negative number.")
    if rounds <= 0:
        raise HTTPException(status_code=400, detail="Rounds must be a positive integer.")
    if not hexToStrategyID:
        return {"updated_strategies": {}, "scores": {}}

    # Prepare hexToStrategy and validate strategy IDs
    initial_hex_data = []
    for hex_id, strat_id in hexToStrategyID.items():
        if strat_id not in idToStrategy:
            raise HTTPException(status_code=400, detail=f"Invalid strategy ID: {strat_id} for hex {hex_id}.")
        strategy_name = idToStrategy[strat_id][0]
        initial_hex_data.append((hex_id, strategy_name))

    # --- Create initial DataFrame ---
    # Schema: hex_id, current_strategy_name
    hex_df = spark.createDataFrame(initial_hex_data, ["hex_id", "current_strategy_name"])
    
    # Broadcast all hex IDs as a set for efficient neighbor lookup within UDFs
    all_hex_ids_set = set(hexToStrategyID.keys())
    all_hex_ids_broadcast = spark.sparkContext.broadcast(all_hex_ids_set)

    # UDF to get direct neighbors for a hex
    get_h3_neighbors_udf = udf(lambda hex_id: get_valid_neighbors_list(hex_id, all_hex_ids_broadcast.value), ArrayType(StringType()))

    # 1. Expand neighbors for each hex
    # This DataFrame will have rows like (hex_id, current_strategy_name, neighbor_hex_id)
    hex_with_neighbors_exploded_df = hex_df.withColumn("neighbor_hex_id", explode(get_h3_neighbors_udf(col("hex_id"))))

    # 2. Join the exploded DataFrame with itself to get strategies for both `hex_id` and `neighbor_hex_id`
    # Use aliases to distinguish columns from different sides of the join
    match_pairs_df = hex_with_neighbors_exploded_df.alias("n") \
        .join(hex_df.alias("s"), col("n.neighbor_hex_id") == col("s.hex_id"), "inner") \
        .select(
            col("n.hex_id").alias("hex1_id"),
            col("n.current_strategy_name").alias("hex1_strategy"),
            col("s.hex_id").alias("hex2_id"),
            col("s.current_strategy_name").alias("hex2_strategy")
        ) \
        .filter(col("hex1_id") < col("hex2_id")) # Filter to ensure unique pairs (A,B but not B,A)

    # --- Play Matches and Calculate Scores ---
    # Apply the UDF to each unique pair
    match_scores_df = match_pairs_df.withColumn(
        "scores_tuple",
        play_match_udf(
            col("hex1_strategy"), col("hex2_strategy"),
            lit(rounds), lit(noise), lit(r), lit(s), lit(t), lit(p)
        )
    ).cache() # Cache results before aggregation

    # Explode scores into individual hex scores
    hex_scores_df = match_scores_df.select(
        col("hex1_id").alias("hex_id"),
        col("scores_tuple")[0].alias("score")
    ).unionAll(
        match_scores_df.select(
            col("hex2_id").alias("hex_id"),
            col("scores_tuple")[1].alias("score")
        )
    )

    # Aggregate total scores per hex
    total_scores_df = hex_scores_df.groupBy("hex_id").agg(spark_sum("score").alias("total_score")).cache()

    # --- Determine Strategy Updates ---
    # Join total scores back to the original hex DataFrame to get current strategies and scores
    hex_with_scores_df = hex_df.join(total_scores_df, "hex_id", "left_outer") \
                               .na.fill(0.0, subset=["total_score"]) # Hexes with no matches get 0 score

    # Prepare a DataFrame of (center_hex_id, neighbor_hex_id) for gathering neighbor details
    neighbor_connections_df = hex_df.withColumn("neighbor_hex_id", explode(get_h3_neighbors_udf(col("hex_id")))) \
                                    .select(col("hex_id").alias("center_hex_id"), col("neighbor_hex_id"))

    # Join `hex_with_scores_df` (aliased as 'n' for neighbors) to `neighbor_connections_df`
    # to get the neighbor's strategy and score.
    hex_with_neighbor_details_for_udf_df = neighbor_connections_df.alias("c") \
        .join(hex_with_scores_df.alias("n"), col("c.neighbor_hex_id") == col("n.hex_id"), "left_outer") \
        .select(
            col("c.center_hex_id").alias("hex_id"), # This is the main hex for which we are calculating
            spark_struct(
                col("n.hex_id").alias("neighbor_hex_id"),
                col("n.current_strategy_name").alias("neighbor_strategy_name"),
                coalesce(col("n.total_score"), lit(0.0)).alias("neighbor_score") # Coalesce for outer join
            ).alias("neighbor_info")
        )

    # Group by `hex_id` and collect all neighbor information into a list
    collected_neighbor_info_df = hex_with_neighbor_details_for_udf_df \
        .groupBy("hex_id") \
        .agg(collect_list("neighbor_info").alias("neighbor_data"))

    # Now, join the `hex_with_scores_df` (aliased as 's' which has the current hex's score and strategy)
    # with the `collected_neighbor_info_df` (aliased as 'n' for neighbor data)
    # to apply the `determine_next_strategy_udf`.
    final_update_df = hex_with_scores_df.alias("s") \
        .join(collected_neighbor_info_df.alias("n"), col("s.hex_id") == col("n.hex_id"), "left_outer") \
        .select(
            col("s.hex_id").alias("hex_id"), # Explicitly select hex_id from 's' to avoid ambiguity
            col("s.current_strategy_name").alias("current_strategy_name"), # Pass through current strategy
            col("s.total_score").alias("total_score"), # Pass through total score
            determine_next_strategy_udf(
                col("s.current_strategy_name"), # center_current_strategy
                col("s.total_score"),            # center_score
                coalesce(col("n.neighbor_data"), lit(spark_array()).cast(neighbor_data_array_type))
            ).alias("updated_strategy")
        )
    
    # If a hex has no neighbors (and thus `neighbor_data` was null), the UDF might return null.
    # In such cases, the strategy should remain unchanged.
    final_update_df = final_update_df.withColumn(
        "updated_strategy",
        spark_when(col("updated_strategy").isNull(), col("current_strategy_name")).otherwise(col("updated_strategy"))
    )

    # Collect results
    updated_strategies_collected = final_update_df.select("hex_id", "updated_strategy").collect()
    updated_strategies: Dict[str, str] = {row.hex_id: row.updated_strategy for row in updated_strategies_collected}

    scores_collected = total_scores_df.select("hex_id", col("total_score").cast(IntegerType()).alias("total_score_int")).collect()
    scores: Dict[str, int] = {row.hex_id: row.total_score_int for row in scores_collected}

    # Unpersist cached DataFrames
    match_scores_df.unpersist()
    total_scores_df.unpersist()

    return {
        "updated_strategies": updated_strategies,
        "scores": scores
    }

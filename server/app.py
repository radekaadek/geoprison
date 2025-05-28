from contextlib import asynccontextmanager
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
from pyspark.sql.functions import col, udf, explode, lit, sum as spark_sum, max as spark_max, when as spark_when, coalesce, struct as spark_struct, collect_list, array as spark_array, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, LongType

spark: SparkSession | None = None

#### Geospatial prisoner's dilemma ####
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifespan of the FastAPI application, handling startup and shutdown events.
    This replaces the deprecated @app.on_event decorators.
    """
    global spark

    # Startup event logic
    print("Application startup initiated.")
    if spark is None:
        spark = SparkSession.builder \
            .appName("GeospatialPrisonersDilemma") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .master("local[15]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR") # Reduce verbosity
        print("SparkSession initialized successfully for local execution.")
    
    # Yield control to the application to handle requests
    yield

    # Shutdown event logic
    print("Application shutdown initiated.")
    if spark is not None:
        spark.stop()
        print("SparkSession stopped.")

# Initialize FastAPI app with the lifespan context manager
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

# --- Helper function to get valid neighbors (Pythonic, used inside UDF) ---
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
# Keeping this here as it was in your original code, but it's not used in the Spark path.
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
def play_match(strat_name1: str, strat_name2: str, rounds: int, noise: float, r: float, s: float, t: float, p: float) -> Tuple[float, float]:
    game = axl.Game(r=r, s=s, t=t, p=p)
    try:
        # Clone players for each match to ensure they start fresh
        player1 = stringToStrat[strat_name1].clone()
        player2 = stringToStrat[strat_name2].clone()
    except KeyError:
        # print(f"Error: Invalid strategy name encountered in UDF: {strat_name1} or {strat_name2}")
        return (0.0, 0.0)

    match = axl.Match(players=(player1, player2), turns=rounds, noise=noise, game=game)
    try:
        match.play()
        final_scores = match.final_score()
        if final_scores is None:
            return (0.0, 0.0)
        return (float(final_scores[0]), float(final_scores[1]))
    except Exception as e:
        # print(f"Error during match between {strat_name1} and {strat_name2}: {e}")
        return (0.0, 0.0)

play_match_udf = udf(play_match, ArrayType(FloatType()))

# --- UDF for Determining Next Strategy ---
def determine_next_strategy(
    center_current_strategy: str,
    center_score: float,
    neighbor_data: List[Dict[str, Any]] # Spark passes Row objects which behave like dicts
) -> str:
    """
    Determines the next strategy for the center_hex based on its score and
    its neighbors' scores.
    """
    max_score = center_score
    next_strategy = center_current_strategy

    if neighbor_data: # Ensure there are neighbors
        for neighbor_info in neighbor_data:
            # Check if neighbor_info is not None (in case of outer join with no valid neighbor)
            # and if 'neighbor_score' key exists and is not None.
            if neighbor_info and 'neighbor_score' in neighbor_info and neighbor_info['neighbor_score'] is not None:
                neighbor_strat = neighbor_info['neighbor_strategy_name']
                neighbor_score = float(neighbor_info['neighbor_score']) # Ensure it's a float for comparison
                if neighbor_score > max_score:
                    max_score = neighbor_score
                    next_strategy = neighbor_strat
    return next_strategy

determine_next_strategy_udf = udf(determine_next_strategy, StringType())

# Define the schema for neighbor_info used within the UDF
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

    start_time = time.time() # Start timing for the entire step

    # --- Input Validation ---
    if noise < 0:
        raise HTTPException(status_code=400, detail="Noise must be a non-negative number.")
    if rounds <= 0:
        raise HTTPException(status_code=400, detail="Rounds must be a positive integer.")
    if not hexToStrategyID:
        print("No hexes provided, returning empty results.")
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
    hex_df = spark.createDataFrame(initial_hex_data, ["hex_id", "current_strategy_name"]).cache()

    # Broadcast all hex IDs as a set for efficient neighbor lookup within UDFs
    all_hex_ids_set = set(hexToStrategyID.keys())
    all_hex_ids_broadcast = spark.sparkContext.broadcast(all_hex_ids_set)

    # UDF to get direct neighbors for a hex
    get_h3_neighbors_udf = udf(lambda hex_id: get_valid_neighbors_list(hex_id, all_hex_ids_broadcast.value), ArrayType(StringType()))

    # --- 1. Generate all (hex, neighbor) pairs ---
    # This exploded DataFrame contains each hex and its valid neighbors within the grid.
    hex_and_its_neighbors_df = hex_df.withColumn(
        "neighbor_hex_id", explode(get_h3_neighbors_udf(col("hex_id")))
    ).cache() # Cache for reuse in both match pairing and neighbor aggregation

    # --- 2. Play Matches and Calculate Scores ---
    # Join the exploded DataFrame with itself to get strategies for both `hex_id` and `neighbor_hex_id`
    # Filter to ensure each pair is processed only once (e.g., A-B, not B-A)
    match_pairs_df = hex_and_its_neighbors_df.alias("h1") \
        .join(hex_df.alias("h2"), col("h1.neighbor_hex_id") == col("h2.hex_id"), "inner") \
        .filter(col("h1.hex_id") < col("h2.hex_id")) \
        .select(
            col("h1.hex_id").alias("hex1_id"),
            col("h1.current_strategy_name").alias("hex1_strategy"),
            col("h2.hex_id").alias("hex2_id"),
            col("h2.current_strategy_name").alias("hex2_strategy")
        ).cache() # Cache match pairs before playing matches

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

    # --- 3. Determine Strategy Updates ---
    # Join total scores back to the original hex DataFrame to get current strategies and scores
    hex_with_current_scores_df = hex_df.join(total_scores_df, "hex_id", "left_outer") \
                                     .na.fill(0.0, subset=["total_score"]) \
                                     .cache() # This DF contains (hex_id, current_strat, current_score)

    # Re-use hex_and_its_neighbors_df to get connections and then join with hex_with_current_scores_df
    # to get details of neighbors (their hex_id, strategy, and score).
    neighbor_details_df = hex_and_its_neighbors_df.alias("center_hex") \
        .join(broadcast(hex_with_current_scores_df.alias("neighbor_hex")), # Broadcast smaller hex_with_current_scores_df
              col("center_hex.neighbor_hex_id") == col("neighbor_hex.hex_id"),
              "left_outer") \
        .select(
            col("center_hex.hex_id").alias("hex_id"), # This is the hex we are determining the strategy for
            spark_struct(
                col("neighbor_hex.hex_id").alias("neighbor_hex_id"),
                col("neighbor_hex.current_strategy_name").alias("neighbor_strategy_name"),
                coalesce(col("neighbor_hex.total_score"), lit(0.0)).alias("neighbor_score") # Coalesce for outer join
            ).alias("neighbor_info")
        )
    
    # Group by `hex_id` and collect all neighbor information into a list
    # The `neighbor_data_array_type` ensures correct schema for the collected list for the UDF.
    collected_neighbor_info_df = neighbor_details_df \
        .groupBy("hex_id") \
        .agg(collect_list("neighbor_info").alias("neighbor_data").cast(neighbor_data_array_type)) \
        .cache() # Cache before final join

    # Now, join the `hex_with_current_scores_df` (which has the current hex's score and strategy)
    # with the `collected_neighbor_info_df` (which has the aggregated neighbor data).
    final_update_df = hex_with_current_scores_df.alias("s") \
        .join(collected_neighbor_info_df.alias("n"), col("s.hex_id") == col("n.hex_id"), "left_outer") \
        .select(
            col("s.hex_id").alias("hex_id"),
            col("s.current_strategy_name").alias("current_strategy_name"),
            col("s.total_score").alias("total_score"),
            determine_next_strategy_udf(
                col("s.current_strategy_name"),
                col("s.total_score"),
                # Ensure the UDF receives an empty list for hexes with no neighbors
                # or no valid neighbor data from the join
                coalesce(col("n.neighbor_data"), lit(spark_array()).cast(neighbor_data_array_type))
            ).alias("updated_strategy")
        )

    # If a hex has no neighbors (and thus `neighbor_data` was null), the UDF might return null.
    # In such cases, the strategy should remain unchanged.
    final_update_df = final_update_df.withColumn(
        "updated_strategy",
        spark_when(col("updated_strategy").isNull(), col("current_strategy_name")).otherwise(col("updated_strategy"))
    ).cache() # Cache final result before collecting

    # Collect results
    updated_strategies_collected = final_update_df.select("hex_id", "updated_strategy").collect()
    updated_strategies: Dict[str, str] = {row.hex_id: row.updated_strategy for row in updated_strategies_collected}

    scores_collected = total_scores_df.select("hex_id", col("total_score").cast(IntegerType()).alias("total_score_int")).collect()
    scores: Dict[str, int] = {row.hex_id: row.total_score_int for row in scores_collected}

    # --- Unpersist cached DataFrames ---
    hex_df.unpersist()
    hex_and_its_neighbors_df.unpersist()
    match_pairs_df.unpersist()
    match_scores_df.unpersist()
    # hex_scores_df is intermediate, unpersisting is good practice if it was cached, but it's not explicitly cached here.
    total_scores_df.unpersist()
    hex_with_current_scores_df.unpersist()
    collected_neighbor_info_df.unpersist()
    final_update_df.unpersist()

    end_time = time.time() # End timing
    print(f"Game step completed in {end_time - start_time:.2f} seconds.")

    return {
        "updated_strategies": updated_strategies,
        "scores": scores
    }

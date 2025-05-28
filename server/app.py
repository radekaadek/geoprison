from contextlib import asynccontextmanager
import axelrod as axl
import h3
import time
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, Callable, Dict, Set, Tuple, FrozenSet, cast, List
from collections import defaultdict
from functools import lru_cache

# --- PySpark Imports ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, pandas_udf, udf, explode, lit, sum as spark_sum,
    max as spark_max, when as spark_when, coalesce, struct as spark_struct,
    collect_list, array as spark_array, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, ArrayType, LongType
)

# Global SparkSession, initialized in lifespan
spark: SparkSession | None = None

# Global UDF variables, assigned in lifespan (for UDFs that don't change per request)
play_match_pandas_udf: Callable | None = None
determine_next_strategy_pandas_udf: Callable | None = None
# get_h3_neighbors_udf is defined locally within game_step for each request
# to correctly capture broadcast variables specific to that request.

# --- Schema Definitions ---
# Schema for individual neighbor information used within the determine_next_strategy UDF
NEIGHBOR_INFO_SCHEMA = StructType([
    StructField("neighbor_hex_id", StringType(), True),
    StructField("neighbor_strategy_name", StringType(), True),
    StructField("neighbor_score", FloatType(), True)
])

# Schema for the array of neighbor data passed to the determine_next_strategy UDF
NEIGHBOR_DATA_ARRAY_TYPE = ArrayType(NEIGHBOR_INFO_SCHEMA)

#### Geospatial Prisoner's Dilemma ####

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifespan of the FastAPI application.
    Handles SparkSession startup and shutdown, and UDF initialization.
    """
    global spark
    global play_match_pandas_udf
    global determine_next_strategy_pandas_udf

    # --- Startup ---
    print("Application startup initiated.")
    if spark is None:
        spark = SparkSession.builder \
            .appName("GeospatialPrisonersDilemma") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .master("local[15]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR") # Reduce console verbosity
        print("SparkSession initialized successfully for local execution.")

        # --- UDF for Playing a Single Match (Pandas UDF) ---
        # This UDF takes series of strategy names and game parameters,
        # plays Axelrod matches, and returns a series of score pairs.
        @pandas_udf(ArrayType(FloatType()))
        def _play_match_pandas_internal(
            strat_name1: pd.Series,
            strat_name2: pd.Series,
            rounds: pd.Series, # Number of rounds per match
            noise: pd.Series,  # Noise level in matches
            r_payoff: pd.Series, s_payoff: pd.Series, t_payoff: pd.Series, p_payoff: pd.Series # Game payoffs (R, S, T, P)
        ) -> pd.Series:
            results = []
            # Parameters are passed as series; extract scalar value (assuming they are constant per batch)
            num_rounds_val = rounds.iloc[0]
            game_noise_val = noise.iloc[0]
            game_r_val = r_payoff.iloc[0]
            game_s_val = s_payoff.iloc[0]
            game_t_val = t_payoff.iloc[0]
            game_p_val = p_payoff.iloc[0]

            game = axl.Game(r=game_r_val, s=game_s_val, t=game_t_val, p=game_p_val)

            for s1_name, s2_name in zip(strat_name1, strat_name2):
                try:
                    player1 = string_to_strategy_object[s1_name].clone()
                    player2 = string_to_strategy_object[s2_name].clone()
                except KeyError:
                    # If a strategy name is not found, assign zero scores.
                    results.append([0.0, 0.0])
                    continue

                match = axl.Match(players=(player1, player2), turns=num_rounds_val, noise=game_noise_val, game=game)
                try:
                    match.play()
                    final_scores = match.final_score_per_turn() # Average score per turn
                    if final_scores is None: # Should not happen if match played
                        results.append([0.0, 0.0])
                    else:
                        # Ensure scores are float, even if Axelrod returns Decimal or other numeric types
                        results.append([float(final_scores[0]), float(final_scores[1])])
                except Exception:
                    # Catch any error during match play (e.g., internal Axelrod issue)
                    # print(f"Error during match play between {s1_name} and {s2_name}: {e}") # Optional: log error
                    results.append([0.0, 0.0]) # Default to zero scores on error
            return pd.Series(results)
        
        play_match_pandas_udf = _play_match_pandas_internal

        # --- UDF for Determining Next Strategy (Pandas UDF) ---
        # This UDF decides if a cell should change its strategy based on its score
        # and the scores of its neighbors.
        @pandas_udf(StringType())
        def _determine_next_strategy_pandas_internal(
            center_current_strategy: pd.Series,
            center_score: pd.Series,
            neighbor_data: pd.Series # Series of lists of neighbor dicts
        ) -> pd.Series:
            next_strategies = []
            for i in range(len(center_current_strategy)):
                current_strat = center_current_strategy.iloc[i]
                current_score = center_score.iloc[i]
                neighbors_info_list = neighbor_data.iloc[i] # This is a list of dicts

                max_observed_score = current_score
                best_strategy_to_adopt = current_strat

                if neighbors_info_list is not None and len(neighbors_info_list) > 0:
                    for neighbor_info_dict in neighbors_info_list:
                        # Validate structure of neighbor_info_dict
                        if (neighbor_info_dict and
                            'neighbor_score' in neighbor_info_dict and
                            neighbor_info_dict['neighbor_score'] is not None and
                            'neighbor_strategy_name' in neighbor_info_dict and
                            neighbor_info_dict['neighbor_strategy_name'] is not None):
                            
                            neighbor_strat = neighbor_info_dict['neighbor_strategy_name']
                            neighbor_score = float(neighbor_info_dict['neighbor_score'])
                            
                            if neighbor_score > max_observed_score:
                                max_observed_score = neighbor_score
                                best_strategy_to_adopt = neighbor_strat
                
                next_strategies.append(best_strategy_to_adopt)
            return pd.Series(next_strategies)

        determine_next_strategy_pandas_udf = _determine_next_strategy_pandas_internal
    
    yield # FastAPI app runs after this point

    # --- Shutdown ---
    print("Application shutdown initiated.")
    if spark is not None:
        spark.stop()
        print("SparkSession stopped.")

# Initialize FastAPI app with the lifespan context manager
app = FastAPI(lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins for simplicity; restrict in production
    allow_credentials=True,
    allow_methods=["*"], # Allows all HTTP methods
    allow_headers=["*"], # Allows all headers
)

# --- Python Helper Function for H3 Neighbors (used inside a Spark UDF) ---
def get_valid_neighbors_list(center_hex: str, all_participating_hexes: Set[str]) -> List[str]:
    """
    Finds H3 k-ring neighbors (ring 1) of center_hex that are also present
    in the set of all_participating_hexes.
    Returns a list of valid neighbor hex IDs.
    """
    try:
        # h3.grid_disk(center_hex, 1) returns the center_hex itself plus its neighbors.
        potential_neighbors = h3.grid_disk(center_hex, 1)
        actual_neighbors = [
            neighbor for neighbor in potential_neighbors
            if neighbor != center_hex and neighbor in all_participating_hexes
        ]
        return actual_neighbors
    except ValueError: # Catches errors from invalid H3 index strings
        # print(f"Warning: Invalid H3 index '{center_hex}' encountered in get_valid_neighbors_list: {e}") # Optional log
        return []

# --- Strategy Definitions ---
# Maps strategy names to their Axelrod library class instances.
string_to_strategy_object: Dict[str, axl.Player] = {
    'Tit-for-Tat': axl.TitForTat(),
    'Random': axl.Random(),
    'Harrington': axl.SecondByHarrington(), # Axelrod's name for a strategy similar to Tit-for-2-Tats
    'Tester': axl.SecondByTester(), # Axelrod's name for a strategy that tests and defects
    'Defector': axl.Defector(),
    'Cooperator': axl.Cooperator(),
    'Alternator': axl.Alternator(),
    'Suspicious Tit-for-Tat': axl.SuspiciousTitForTat(),
    'Forgiving Tit-for-Tat': axl.ForgivingTitForTat(),
    'Grudger': axl.Grudger(),
}

# Type alias for strategy ID mapping
StrategyIdMapType = Dict[int, Tuple[str, str]]

# Maps integer IDs (from frontend/API) to strategy names and display colors.
id_to_strategy_info: StrategyIdMapType = {
    0: ("Alternator", "gray"),
    1: ("Cooperator", "green"),
    2: ("Defector", "pink"),
    3: ("Forgiving Tit-for-Tat", "brown"),
    4: ("Grudger", "orange"),
    5: ("Harrington", "red"),
    6: ("Random", "black"),
    7: ("Suspicious Tit-for-Tat", "purple"),
    8: ("Tester", "yellow"),
    9: ("Tit-for-Tat", "blue"),
}

# --- API Endpoints ---
@app.get("/")
async def root():
    """ Basic root endpoint for the API. """
    return {"message": "Geospatial Prisoner's Dilemma API"}

@app.get("/strategies", response_model=StrategyIdMapType)
async def strategies() -> StrategyIdMapType:
    """ Returns the available strategies with their names and suggested colors. """
    return id_to_strategy_info

@app.post("/game_step")
async def game_step(
    hex_to_strategy_id_map: Dict[str, int],
    rounds: int = 15,
    noise: float = 0.0,
    r_payoff: float = 3, # Reward for mutual cooperation
    s_payoff: float = 0, # Sucker's payoff (cooperate while other defects)
    t_payoff: float = 5, # Temptation to defect
    p_payoff: float = 1  # Punishment for mutual defection
):
    """
    Simulates one step (generation) of the spatial prisoner's dilemma using PySpark.
    Each hex plays against its immediate neighbors.
    Scores are calculated, and then hexes update their strategy based on the
    highest-scoring strategy (their own or a neighbor's).
    """
    global spark
    global play_match_pandas_udf
    global determine_next_strategy_pandas_udf

    if spark is None:
        raise HTTPException(status_code=503, detail="SparkSession not initialized. Service unavailable.")
    if play_match_pandas_udf is None or determine_next_strategy_pandas_udf is None:
        raise HTTPException(status_code=503, detail="Core UDFs not initialized. Service unavailable.")

    start_time_simulation = time.time()

    # --- Input Validation ---
    if not (0 <= noise <= 1): # Noise is typically a probability
        raise HTTPException(status_code=400, detail="Noise must be between 0.0 and 1.0 inclusive.")
    if rounds <= 0:
        raise HTTPException(status_code=400, detail="Rounds must be a positive integer.")
    if not hex_to_strategy_id_map:
        print("No hexes provided for game step, returning empty results.")
        return {"updated_strategies": {}, "scores": {}}

    # --- 1. Prepare Initial DataFrame from Input ---
    # Convert input map (hex_id -> strategy_id) to (hex_id, strategy_name) tuples
    initial_hex_data_tuples = []
    for hex_id_str, strat_id_int in hex_to_strategy_id_map.items():
        if strat_id_int not in id_to_strategy_info:
            raise HTTPException(status_code=400, detail=f"Invalid strategy ID: {strat_id_int} for hex {hex_id_str}.")
        strategy_name, _ = id_to_strategy_info[strat_id_int]
        initial_hex_data_tuples.append((hex_id_str, strategy_name))

    # Create Spark DataFrame: (hex_id, current_strategy_name)
    # This DataFrame represents the current state of the game grid.
    current_grid_state_df = spark.createDataFrame(
        initial_hex_data_tuples,
        ["hex_id", "current_strategy_name"]
    ).cache() # Cache as it will be used multiple times

    # Broadcast the set of all active hex IDs for efficient lookup in UDF
    all_hex_ids_in_grid_set = set(hex_to_strategy_id_map.keys())
    all_hex_ids_broadcast = spark.sparkContext.broadcast(all_hex_ids_in_grid_set)

    # --- Define UDF for finding neighbors (locally to capture broadcast) ---
    # This UDF uses the broadcasted set of all hexes to find valid neighbors.
    @udf(ArrayType(StringType()))
    def get_h3_valid_neighbors_udf(center_hex_id_str: str) -> List[str]:
        return get_valid_neighbors_list(center_hex_id_str, all_hex_ids_broadcast.value)

    # --- 2. Identify All Hex-Neighbor Pairs ---
    # Explode hexes to list their valid neighbors within the grid.
    # Result: (hex_id, current_strategy_name, neighbor_hex_id)
    hex_with_its_neighbors_df = current_grid_state_df.withColumn(
        "neighbor_hex_id", explode(get_h3_valid_neighbors_udf(col("hex_id")))
    ).cache() # Cache as this is a key structural DataFrame

    # --- 3. Prepare Match Pairs and Play Matches ---
    # Join hex_with_its_neighbors_df with current_grid_state_df to get strategies for both players in a match.
    # Filter (h1.hex_id < h2.hex_id) ensures each pair plays only once (e.g., A-B, not B-A too).
    # Result: (hex1_id, hex1_strategy, hex2_id, hex2_strategy)
    match_pairs_for_game_df = hex_with_its_neighbors_df.alias("h1_center") \
        .join(
            current_grid_state_df.alias("h2_neighbor_info"),
            col("h1_center.neighbor_hex_id") == col("h2_neighbor_info.hex_id"),
            "inner" # Only consider neighbors that are part of the grid state
        ) \
        .filter(col("h1_center.hex_id") < col("h2_neighbor_info.hex_id")) \
        .select(
            col("h1_center.hex_id").alias("hex1_id"),
            col("h1_center.current_strategy_name").alias("hex1_strategy"),
            col("h2_neighbor_info.hex_id").alias("hex2_id"),
            col("h2_neighbor_info.current_strategy_name").alias("hex2_strategy")
        ).cache() # Cache before executing the match UDF

    # Play matches using the play_match_pandas_udf.
    # Result: (hex1_id, hex1_strategy, hex2_id, hex2_strategy, match_scores_tuple)
    # where match_scores_tuple is [score_for_hex1, score_for_hex2]
    match_results_df = match_pairs_for_game_df.withColumn(
        "match_scores_tuple",
        play_match_pandas_udf(
            col("hex1_strategy"), col("hex2_strategy"),
            lit(rounds), lit(noise),
            lit(r_payoff), lit(s_payoff), lit(t_payoff), lit(p_payoff)
        )
    ).cache() # Cache match results

    # --- 4. Calculate Total Scores for Each Hex ---
    # Unpack scores: create one row for each player's score from each match.
    # Result for hex1: (hex_id (was hex1_id), score (was scores_tuple[0]))
    # Result for hex2: (hex_id (was hex2_id), score (was scores_tuple[1]))
    scores_player1_df = match_results_df.select(
        col("hex1_id").alias("hex_id"),
        col("match_scores_tuple")[0].alias("individual_match_score")
    )
    scores_player2_df = match_results_df.select(
        col("hex2_id").alias("hex_id"),
        col("match_scores_tuple")[1].alias("individual_match_score")
    )
    
    # Combine all individual scores and sum them up per hex.
    # Result: (hex_id, total_accumulated_score)
    all_individual_scores_df = scores_player1_df.unionAll(scores_player2_df)
    total_scores_per_hex_df = all_individual_scores_df \
        .groupBy("hex_id") \
        .agg(spark_sum("individual_match_score").alias("total_accumulated_score")) \
        .cache() # Cache total scores

    # --- 5. Prepare Data for Strategy Update Decision ---
    # Join total scores back to the original grid state.
    # Result: (hex_id, current_strategy_name, total_accumulated_score)
    # Use left_outer join in case some hexes played no matches (e.g., isolated).
    # Fill potential null scores with 0.0 for such hexes.
    grid_with_current_scores_df = current_grid_state_df \
        .join(total_scores_per_hex_df, "hex_id", "left_outer") \
        .na.fill(0.0, subset=["total_accumulated_score"]) \
        .cache() # Cache this comprehensive state

    # Gather neighbor information: for each hex, collect its neighbors' IDs, strategies, and scores.
    # Re-use hex_with_its_neighbors_df (hex_id, its_neighbor_hex_id)
    # Join with grid_with_current_scores_df to fetch neighbor details.
    # Result: (center_hex_id, neighbor_info_struct)
    # where neighbor_info_struct contains (neighbor_actual_hex_id, neighbor_strategy, neighbor_score)
    neighbor_details_for_aggregation_df = hex_with_its_neighbors_df.alias("center_hex_connections") \
        .join(
            broadcast(grid_with_current_scores_df.alias("neighbor_hex_info")), # Broadcast smaller table
            col("center_hex_connections.neighbor_hex_id") == col("neighbor_hex_info.hex_id"),
            "left_outer" # Use left_outer to ensure all connections are kept even if a neighbor somehow misses a score
        ) \
        .select(
            col("center_hex_connections.hex_id").alias("center_hex_id"),
            spark_struct( # Create a struct for each neighbor's data
                col("neighbor_hex_info.hex_id").alias("neighbor_hex_id"), # Actual ID of the neighbor
                col("neighbor_hex_info.current_strategy_name").alias("neighbor_strategy_name"),
                coalesce(col("neighbor_hex_info.total_accumulated_score"), lit(0.0)).alias("neighbor_score")
            ).alias("neighbor_complete_info")
        )

    # Aggregate all neighbor_info_structs into a list for each center_hex_id.
    # Result: (center_hex_id, list_of_neighbor_data)
    # The list_of_neighbor_data matches NEIGHBOR_DATA_ARRAY_TYPE for the UDF.
    aggregated_neighbor_data_df = neighbor_details_for_aggregation_df \
        .groupBy("center_hex_id") \
        .agg(collect_list("neighbor_complete_info").alias("list_of_neighbor_data")) \
        .withColumnRenamed("center_hex_id", "hex_id") \
        .withColumn("list_of_neighbor_data", col("list_of_neighbor_data").cast(NEIGHBOR_DATA_ARRAY_TYPE)) \
        .cache() # Cache before final join

    # --- 6. Determine Next Strategy for Each Hex ---
    # Join the grid_with_current_scores_df (center hex's own data)
    # with aggregated_neighbor_data_df (data of its neighbors).
    # This provides all necessary inputs to the determine_next_strategy_pandas_udf.
    # Use left_outer join in case a hex has no neighbors (its entry would be missing from aggregated_neighbor_data_df).
    data_for_strategy_update_df = grid_with_current_scores_df.alias("center_hex_data") \
        .join(
            aggregated_neighbor_data_df.alias("neighbors_summary"),
            col("center_hex_data.hex_id") == col("neighbors_summary.hex_id"),
            "left_outer"
        ) \
        .select(
            col("center_hex_data.hex_id").alias("hex_id"),
            col("center_hex_data.current_strategy_name"),
            col("center_hex_data.total_accumulated_score"),
            # Ensure UDF gets an empty list if no neighbors (coalesce null from left_outer join)
            coalesce(
                col("neighbors_summary.list_of_neighbor_data"),
                lit(spark_array()).cast(NEIGHBOR_DATA_ARRAY_TYPE) # Empty array with correct schema
            ).alias("final_neighbor_data_list")
        )

    # Apply the UDF to determine the updated strategy.
    # Result: (hex_id, current_strategy_name, total_accumulated_score, updated_strategy_name)
    final_updated_grid_df = data_for_strategy_update_df.withColumn(
        "updated_strategy_name",
        determine_next_strategy_pandas_udf(
            col("current_strategy_name"),
            col("total_accumulated_score"),
            col("final_neighbor_data_list")
        )
    ).cache() # Cache final result before collecting

    # Defensive: Ensure updated_strategy is not null. If UDF somehow returns null, keep current strategy.
    # (The UDF is designed not to return nulls, but this is an extra safeguard).
    final_updated_grid_df = final_updated_grid_df.withColumn(
        "updated_strategy_name",
        spark_when(col("updated_strategy_name").isNull(), col("current_strategy_name"))
        .otherwise(col("updated_strategy_name"))
    )
    
    # --- 7. Collect Results and Format Output ---
    # Collect updated strategies: (hex_id, updated_strategy_name)
    # .collect() brings data to the driver; be mindful of very large grids.
    updated_strategies_list = final_updated_grid_df.select("hex_id", "updated_strategy_name").collect()
    updated_strategies_output_map: Dict[str, str] = {
        row.hex_id: row.updated_strategy_name for row in updated_strategies_list
    }

    # Collect scores: (hex_id, total_accumulated_score)
    # Ensure scores are integers for the output as per original code.
    scores_list = total_scores_per_hex_df.select(
        "hex_id",
        col("total_accumulated_score").cast(IntegerType()).alias("final_score_int")
    ).collect()
    scores_output_map: Dict[str, int] = {
        row.hex_id: row.final_score_int for row in scores_list
    }

    # --- 8. Clean Up: Unpersist Cached DataFrames ---
    current_grid_state_df.unpersist()
    hex_with_its_neighbors_df.unpersist()
    match_pairs_for_game_df.unpersist()
    match_results_df.unpersist()
    # all_individual_scores_df is intermediate and not explicitly cached
    total_scores_per_hex_df.unpersist()
    grid_with_current_scores_df.unpersist()
    aggregated_neighbor_data_df.unpersist()
    final_updated_grid_df.unpersist()
    all_hex_ids_broadcast.unpersist() # Clean up broadcast variable

    end_time_simulation = time.time()
    print(f"Game step simulation completed in {end_time_simulation - start_time_simulation:.2f} seconds.")

    return {
        "updated_strategies": updated_strategies_output_map,
        "scores": scores_output_map
    }

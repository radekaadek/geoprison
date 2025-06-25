import axelrod as axl
import h3
import time # Not strictly used in the provided snippet, but often useful
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import FrozenSet
from collections import defaultdict
from functools import lru_cache # Import lru_cache

#### Geospatial prisoner's dilemma ####

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

# --- Helper function to get valid neighbors ---
def get_valid_neighbors(center_hex: str, all_hexes: set[str]) -> set[str]:
    """
    Finds neighbors of center_hex that are also in the all_hexes set.
    h3.grid_disk can raise H3CellError (a ValueError subclass) if center_hex is invalid.
    """
    # This will raise h3.H3CellError if center_hex is not a valid H3 cell index string
    potential_neighbors = h3.grid_disk(center_hex, 1)
    actual_neighbors = {neighbor for neighbor in potential_neighbors
                        if neighbor != center_hex and neighbor in all_hexes}
    return actual_neighbors

# --- Cached function to calculate neighbor counts ---
@lru_cache(maxsize=10)
def _get_cached_hex_neighbors_counts(hex_ids_fset: FrozenSet[str]) -> dict[str, int]:
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
    # print(f"DEBUG: Cache MISS for _get_cached_hex_neighbors_counts with: {hex_ids_fset}") # For debugging cache behavior
    hex_ids_set = set(hex_ids_fset) # Convert to set for efficient use in get_valid_neighbors
    counts: Dict[str, int] = {}
    for hex_id in hex_ids_fset:
        try:
            # get_valid_neighbors calls h3.grid_disk, which can raise H3CellError (ValueError)
            neighbors = get_valid_neighbors(hex_id, hex_ids_set)
            counts[hex_id] = len(neighbors)
        except ValueError as e: # Catch H3CellError or other ValueErrors from h3 library
            # print(f"DEBUG: Invalid H3 index '{hex_id}' encountered in _get_cached_hex_neighbors_counts: {e}")
            # Re-raise the error with context. The caller (game_step) will handle converting it to HTTPException.
            raise ValueError(f"Invalid H3 index '{hex_id}': {e}") from e
    return counts


# --- Strategy Definitions ---
stringToStrat: dict[str, axl.Player] = {
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

idStrategyType = dict[int, tuple[str, str]]

StrategyIdMapType = dict[int, tuple[str, str]]

idToStrategy: StrategyIdMapType = {
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
    """ Basic root endpoint. """
    return {"message": "Geospatial Prisoner's Dilemma API"}

@app.get("/strategies")
async def strategies() -> idStrategyType:
    """ Returns the mapping of strategy IDs to strategy names and colors. """
    return idToStrategy

@app.post("/game_step")
async def game_step(hexToStrategyID: dict[str, int], rounds: int = 15, noise: float = 0.0, r: float = 3, s: float = 0, t: float = 5, p: float = 1):
    """
    Simulates one step of the spatial prisoner's dilemma.
    """
    # --- Input Validation and Setup ---
    if noise < 0:
        raise HTTPException(status_code=400, detail="Noise must be a non-negative number.")
    if rounds <= 0: # Axelrod turns should be positive
        raise HTTPException(status_code=400, detail="Rounds must be a positive integer.")

    start_time_simulation = time.time()

    gameToUse = axl.Game(r=r, s=s, t=t, p=p)

    hexToStrategy: dict[str, str] = {}
    for hexID, stratID in hexToStrategyID.items():
        if stratID not in idToStrategy:
            raise HTTPException(status_code=400, detail=f"Invalid strategy ID: {stratID} for hex {hexID}.")
        hexToStrategy[hexID] = idToStrategy[stratID][0]

    all_hex_ids: set[str] = set(hexToStrategy.keys())
    if not all_hex_ids:
        # If there are no hexes, return an empty/appropriate response
        return {"updated_strategies": {}, "scores": {}}
        # Or raise HTTPException(status_code=400, detail="No hexagons provided.")

    # --- Get Neighbor Counts (Cached) ---
    all_hex_ids_frozen = frozenset(all_hex_ids)
    try:
        # Call the cached function to get neighbor counts
        hexesToNumNeighbors = _get_cached_hex_neighbors_counts(all_hex_ids_frozen)
    except ValueError as e:
        # If _get_cached_hex_neighbors_counts raises ValueError (e.g., for invalid H3 index),
        # convert it to an HTTPException.
        raise HTTPException(status_code=400, detail=str(e))

    # --- Initialize Players ---
    hexToPlayer: dict[str, axl.Player] = {}
    for hex_id, strategy_name in hexToStrategy.items():
        # stringToStrat should contain all strategy names from idToStrategy
        if strategy_name not in stringToStrat:
            # This would indicate an inconsistency between idToStrategy and stringToStrat
            raise HTTPException(status_code=500, detail=f"Internal server error: Strategy '{strategy_name}' not found in stringToStrat map.")
        try:
            # Clone players to ensure independent states for each match
            hexToPlayer[hex_id] = stringToStrat[strategy_name].clone()
        except AttributeError: # Should not happen with standard Axelrod players
            hexToPlayer[hex_id] = stringToStrat[strategy_name]


    # --- Play Matches and Calculate Total Scores ---
    hexToTotalScore = defaultdict(float) # Use float for scores, then cast to int
    played_pairs: set[tuple[str, str]] = set()

    for center_hex in all_hex_ids:
        center_player = hexToPlayer[center_hex]
        
        # Optimization: if a hex has no neighbors, it plays no games with others.
        # Its score will remain 0 unless it plays against itself (not typical in this model).
        if hexesToNumNeighbors.get(center_hex, 0) == 0:
            continue # No neighbors to play against

        # We need the actual neighbors, not just the count here.
        # This call is not cached per hex_id, but the overall structure is determined by all_hex_ids.
        # get_valid_neighbors itself is relatively cheap if h3.grid_disk is fast for single hexes.
        neighbors_of_center = get_valid_neighbors(center_hex, all_hex_ids)

        for neighbor_hex in neighbors_of_center:
            pair = tuple(sorted((center_hex, neighbor_hex)))
            if pair in played_pairs:
                continue # This pair has already played in this step
            
            played_pairs.add(pair)
            neighbor_player = hexToPlayer[neighbor_hex]

            # Axelrod Match setup
            # Players are already cloned, so they have fresh state for each set of interactions.
            # If strategies had memory across different opponents within the same game_step,
            # further resets might be needed, but clone() usually suffices.
            match = axl.Match(players=(center_player, neighbor_player), turns=rounds, noise=noise, game=gameToUse)
            try:
                match.play()
            except Exception as e:
                # Catch potential errors during match play if any strategy behaves unexpectedly
                # This is a safeguard; robust strategies shouldn't error here.
                raise HTTPException(status_code=500, detail=f"Error during match between {center_hex} ({hexToStrategy[center_hex]}) and {neighbor_hex} ({hexToStrategy[neighbor_hex]}): {e}")

            final_scores = match.final_score() # Returns a tuple of total scores (player1_score, player2_score)
            
            if final_scores is None: # Should not happen if turns > 0
                score_center_player, score_neighbor_player = 0.0, 0.0
            else:
                # Ensure scores are float for accumulation
                score_center_player, score_neighbor_player = float(final_scores[0]), float(final_scores[1])

            hexToTotalScore[center_hex] += score_center_player
            hexToTotalScore[neighbor_hex] += score_neighbor_player

    # --- Determine Strategy Updates for the Next Step ---
    nextHexToStrategy = hexToStrategy.copy() # Start with current strategies

    for center_hex in all_hex_ids:
        center_original_score = hexToTotalScore[center_hex]
        
        # If a hex has no neighbors, its strategy won't change based on neighbor comparison.
        if hexesToNumNeighbors.get(center_hex, 0) == 0:
            continue

        neighbors_of_center = get_valid_neighbors(center_hex, all_hex_ids)
        # This check is mostly redundant due to the hexesToNumNeighbors check above, but safe.
        if not neighbors_of_center: 
            continue

        # Initialize 'best' with the current hex's own score and strategy
        max_score_among_self_and_neighbors = center_original_score
        strategy_of_max_scorer = hexToStrategy[center_hex]

        for neighbor_hex in neighbors_of_center:
            neighbor_total_score = hexToTotalScore[neighbor_hex]
            if neighbor_total_score > max_score_among_self_and_neighbors:
                max_score_among_self_and_neighbors = neighbor_total_score
                strategy_of_max_scorer = hexToStrategy[neighbor_hex] # Original strategy of the best neighbor

        # Update strategy if a neighbor (or itself, if it somehow changed strategy and won, which isn't this model)
        # scored strictly higher than the center_hex's original score.
        # The key is that `strategy_of_max_scorer` might be different from `hexToStrategy[center_hex]`.
        if max_score_among_self_and_neighbors > center_original_score:
            nextHexToStrategy[center_hex] = strategy_of_max_scorer
        # If tied, or if its own score was highest, it keeps its current strategy (already in nextHexToStrategy).

    # --- Prepare Response ---
    # Convert scores to integers for the response
    hexToTotalScore_int: dict[str, int] = {hexID: int(round(score)) for hexID, score in hexToTotalScore.items()}
    
    # Ensure all original hexes are in the scores dictionary, even if they played no games (e.g., isolated hexes)
    # or had a score of 0. defaultdict handles unplayed hexes as 0.
    for hexID in all_hex_ids:
        if hexID not in hexToTotalScore_int:
            hexToTotalScore_int[hexID] = 0

    end_time_simulation = time.time()
    print(f"Game step simulation completed in {end_time_simulation - start_time_simulation:.2f} seconds.")

    response = {
        "updated_strategies": nextHexToStrategy,
        "scores": hexToTotalScore_int
    }
    return response


import axelrod as axl
import h3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Set, Tuple
from collections import defaultdict # Useful for accumulating scores

#### Geospatial prisoner's dilemma ####

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_valid_neighbors(center_hex: str, all_hexes: Set[str]) -> Set[str]:
    """Finds neighbors of center_hex that are also in the all_hexes set."""
    potential_neighbors = h3.grid_disk(center_hex, 1)
    actual_neighbors = {neighbor for neighbor in potential_neighbors
                        if neighbor != center_hex and neighbor in all_hexes}
    return actual_neighbors

# Map strategy names to Axelrod strategy objects
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
    # Add more strategies as needed
}

@app.post("/game_step") # Renamed for clarity as it performs one step
async def game_step(hexToStrategy: Dict[str, str], rounds: int = 15):
    """
    Simulates one step of the spatial prisoner's dilemma.
    Each hexagon plays against its neighbors for the specified number of rounds.
    Scores are calculated, and hexagons adopt the strategy of their
    highest-scoring neighbor if that neighbor scored strictly higher.

    Args:
        hexToStrategy: A dictionary where keys are H3 hex IDs (strings)
                       and values are strategy names (strings).
        rounds: The number of rounds for each pairwise prisoner's dilemma match.

    Returns:
        A dictionary containing the updated strategy map for the next step
        and the total scores achieved by each hexagon in this step.
    """
    print(f"Received request: {rounds=}, strategies for {len(hexToStrategy)} hexes")

    all_hex_ids = set(hexToStrategy.keys())
    if not all_hex_ids:
        return {"updated_strategies": {}, "scores": {}, "message": "No hexagons provided."}

    hexToPlayer = {}
    for hex_id, strategy_name in hexToStrategy.items():
        if strategy_name not in stringToStrat:
            # Handle unknown strategy name
            print(f"Error: Unknown strategy '{strategy_name}' for hex {hex_id}")
            # Option 1: Skip this hex (might cause issues if neighbors expect it)
            # Option 2: Assign a default strategy
            # Option 3: Return an error response
            return {"error": f"Unknown strategy name: {strategy_name}"} # Error response
        # Use clone() to ensure each player has an independent state,
        # especially important if running multiple steps or complex strategies.
        try:
             hexToPlayer[hex_id] = stringToStrat[strategy_name].clone()
        except AttributeError:
             # Fallback if clone isn't available (older Axelrod?) or for simple types
             hexToPlayer[hex_id] = stringToStrat[strategy_name]


    # --- 2. Play Matches and Calculate Total Scores ---
    # Use defaultdict to easily accumulate scores
    hexToTotalScore = defaultdict(int)
    # Keep track of played pairs to avoid double-counting interactions
    # within this step's score calculation (A vs B is the same interaction as B vs A)
    played_pairs: Set[Tuple[str, str]] = set()

    for center_hex in all_hex_ids:
        neighbors = get_valid_neighbors(center_hex, all_hex_ids)
        center_player = hexToPlayer[center_hex]

        for neighbor_hex in neighbors:
            # Create a sorted tuple to represent the pair uniquely
            pair = tuple(sorted((center_hex, neighbor_hex)))
            if pair in played_pairs:
                continue # Already processed this interaction

            played_pairs.add(pair)
            neighbor_player = hexToPlayer[neighbor_hex]

            # Create and play the match
            # Ensure players are reset if necessary (clone usually handles this)
            # center_player.reset() # Generally not needed with clone()
            # neighbor_player.reset()
            match = axl.Match(players=(center_player, neighbor_player), turns=rounds)
            match.play()

            # Get scores - assumes center_player is player 0, neighbor_player is player 1
            # Verify this assumption based on how players were passed to axl.Match
            score_center, score_neighbor = match.final_score()

            # Add scores to the respective hexagons' totals
            hexToTotalScore[center_hex] += score_center
            hexToTotalScore[neighbor_hex] += score_neighbor

    # --- 3. Determine Strategy Updates for the Next Step ---
    nextHexToStrategy = hexToStrategy.copy() # Start with current strategies

    for center_hex in all_hex_ids:
        center_score = hexToTotalScore[center_hex] # Use defaultdict's 0 default if hex played no games
        neighbors = get_valid_neighbors(center_hex, all_hex_ids)

        if not neighbors:
            continue # No neighbors to compare with, strategy remains unchanged

        max_neighbor_score = -float('inf') # Initialize very low
        best_neighbor_strategy_name = None # Strategy name of the best neighbor

        # Find the highest score among neighbors
        for neighbor_hex in neighbors:
            neighbor_score = hexToTotalScore[neighbor_hex]
            if neighbor_score > max_neighbor_score:
                max_neighbor_score = neighbor_score
                # Store the *original* strategy name of this best-performing neighbor
                best_neighbor_strategy_name = hexToStrategy[neighbor_hex]
            # Optional: Handle ties (e.g., randomly pick one, or stick with current)
            # Current logic picks the first one encountered with the max score.

        # --- 4. Update Strategy if a Neighbor Did Better ---
        # Check if found a neighbor AND that neighbor scored strictly higher
        if best_neighbor_strategy_name is not None and max_neighbor_score > center_score:
            nextHexToStrategy[center_hex] = best_neighbor_strategy_name
            # print(f"Hex {center_hex} switching from {hexToStrategy[center_hex]} (score {center_score}) to {best_neighbor_strategy_name} (neighbor score {max_neighbor_score})")


    # --- 5. Return Results ---
    # convert scores to std: int instead of numpy.int64
    hexToTotalScore = dict(hexToTotalScore)
    for hexID, score in hexToTotalScore.items():
        hexToTotalScore[hexID] = int(score)
    response = {
        "updated_strategies": nextHexToStrategy,
        # Convert defaultdict back to regular dict for JSON compatibility
        "scores": hexToTotalScore
    }
    print("Game step processed.")
    print(f"{response=}") # Can be verbose, uncomment for debugging
    return response


import axelrod as axl
import h3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Set

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
    potential_neighbors = h3.grid_disk(center_hex, 1) # Includes center
    actual_neighbors = {neighbor for neighbor in potential_neighbors
                        if neighbor != center_hex and neighbor in all_hexes}
    return actual_neighbors

stringToStrat = {'Tit-for-tat': axl.TitForTat(), 'Random': axl.Random(), 'Harrington': axl.SecondByHarrington(), 'Tester': axl.SecondByTester(), 'Suspicious tit-for-tat': axl.SuspiciousTitForTat(), 'Forgiving tit-for-tat': axl.ForgivingTitForTat()}

@app.get("/")
async def root() -> Dict[str, str]:
    return {"message": "Hello"}

@app.post("/game")
async def game(hexToStrategy: Dict[str, str], rounds: int = 15):
    """
    Handles a game request with specified strategies and rounds.

    Args:
        hexToStrategy: A dictionary where keys are hexIDs and values are strategies.
        rounds: The number of rounds to play.

    Returns:
        A greeting message (or game results when implemented).
    """
    response = {"message": "Game processed", "rounds": rounds, "strategies": hexToStrategy}
    print(f"Received request, {response=}")
    hexToStrat = map(lambda x: stringToStrat[x], hexToStrategy.values())
    hexToNeighbors = map(lambda x: get_valid_neighbors(x, set(hexToStrategy.keys())), hexToStrategy.keys())

    # Play the game
    results = []

    return response

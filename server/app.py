import axelrod as axl
import h3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
from pydantic import BaseModel

#### Geospatial prisoner's dilemma ####

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create a list of strategies
strategies = [axl.TitForTat(), axl.Random(), axl.Random(), axl.ForgivingTitForTat()]

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
    return response

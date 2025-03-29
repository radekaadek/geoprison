import axelrod as axl
import h3
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

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

# Wait for connections
@app.get("/run_game")
async def run_game(hex_to_strategy: dict[str, str] = dict()):
    # Create a list of players
    # Server-Sent Events (SEE)
    pass


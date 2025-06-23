import matplotlib.pyplot as plt
import collections
import json

def visualize_strategies_per_round(states_data):
    """
    Visualizes the number of players for each strategy across different rounds.

    Args:
        states_data (list): A list of dictionaries, where each dictionary
                            represents a round. Each round's dictionary maps
                            player IDs to a dictionary containing 'strategy' and 'score'.
                            Example:
                            [
                                {
                                    "player1_id": {"strategy": "Tit-for-Tat", "score": 0},
                                    "player2_id": {"strategy": "Cooperator", "score": 0}
                                },
                                {
                                    "player1_id": {"strategy": "Tit-for-Tat", "score": 10},
                                    "player2_id": {"strategy": "Cooperator", "score": 5}
                                }
                            ]
    """
    if not states_data:
        print("No data provided to visualize.")
        return

    # Dictionary to store strategy counts per round
    # Format: {round_index: {strategy_name: count}}
    strategy_counts_per_round = collections.defaultdict(lambda: collections.defaultdict(int))

    # Collect all unique strategy names
    all_strategies = set()

    for round_idx, round_state in enumerate(states_data):
        for player_id, player_info in round_state.items():
            strategy = player_info.get("strategy")
            if strategy:
                strategy_counts_per_round[round_idx][strategy] += 1
                all_strategies.add(strategy)

    # Prepare data for plotting
    rounds = sorted(strategy_counts_per_round.keys())
    
    if not rounds:
        print("No rounds found with strategy data.")
        return

    # Create a dictionary to hold the counts for each strategy over rounds
    # Format: {strategy_name: [count_round0, count_round1, ...]}
    plot_data = collections.defaultdict(list)

    for round_idx in rounds:
        for strategy in all_strategies:
            plot_data[strategy].append(strategy_counts_per_round[round_idx].get(strategy, 0))

    # Plotting
    plt.figure(figsize=(12, 7))

    for strategy, counts in plot_data.items():
        plt.plot(rounds, counts, marker='o', label=strategy)

    plt.xlabel("Round Number")
    plt.ylabel("Number of Players")
    plt.title("Number of Players per Strategy Over Rounds")
    plt.xticks(rounds) # Ensure all round numbers are shown on x-axis
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(title="Strategy")
    plt.tight_layout()
    plt.show()

# --- Example Usage ---
# You can replace this sample_states_data with your actual data.
# The data provided in the prompt was incomplete, so this is a fabricated example
# to demonstrate the script's functionality.
# sample_states_data = [
#     # Round 0
#     {
#         "84525c9ffffffff": {"strategy": "Forgiving Tit-for-Tat", "score": 0},
#         "8452451ffffffff": {"strategy": "Tit-for-Tat", "score": 0},
#         "84534b1ffffffff": {"strategy": "Cooperator", "score": 0},
#         "84525d7ffffffff": {"strategy": "Alternator", "score": 0},
#         "84526a1ffffffff": {"strategy": "Tit-for-Tat", "score": 0},
#         "8443b0bffffffff": {"strategy": "Harrington", "score": 0},
#         "84526bdffffffff": {"strategy": "Cooperator", "score": 0},
#         "8452601ffffffff": {"strategy": "Harrington", "score": 0},
#         "8452489ffffffff": {"strategy": "Suspicious Tit-for-Tat", "score": 0}
#     },
#     # Round 1 (some changes)
#     {
#         "p1": {"strategy": "Forgiving Tit-for-Tat", "score": 10},
#         "p2": {"strategy": "Tit-for-Tat", "score": 12},
#         "p3": {"strategy": "Cooperator", "score": 8},
#         "p4": {"strategy": "Cooperator", "score": 7},
#         "p5": {"strategy": "Alternator", "score": 9},
#         "p6": {"strategy": "Harrington", "score": 11},
#         "p7": {"strategy": "Tit-for-Tat", "score": 15}
#     },
#     # Round 2 (more changes)
#     {
#         "a1": {"strategy": "Tit-for-Tat", "score": 20},
#         "a2": {"strategy": "Tit-for-Tat", "score": 25},
#         "a3": {"strategy": "Cooperator", "score": 18},
#         "a4": {"strategy": "Forgiving Tit-for-Tat", "score": 22},
#         "a5": {"strategy": "Harrington", "score": 19}
#     }
# ]

# read from json instead of from a string

with open('states.json') as f:
    sample_states_data = json.load(f)['states']

# If your data is in a string format, you might need to parse it first:
# data_string = '{"states": [{"84525c9ffffffff":{"strategy":"Forgiving Tit-for-Tat","score":0}, ... }]}'
# parsed_data = json.loads(data_string)
# actual_states_data = parsed_data.get('states', [])

# Call the visualization function with your data
visualize_strategies_per_round(sample_states_data)


import matplotlib.pyplot as plt
import collections
import json
import os

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

    # Translations and colors for strategy names
    strategy_info = {
        "Alternator": {"translation": "Alternator", "color": "gray"},
        "Harrington": {"translation": "Harrington", "color": "red"},
        "Cooperator": {"translation": "Kooperant", "color": "green"},
        "Random": {"translation": "Losowy", "color": "black"},
        "Grudger": {"translation": "Mściwy", "color": "orange"},
        "Suspicious Tit-for-Tat": {"translation": "Podejrzliwy Wet za Wet", "color": "purple"},
        "Tester": {"translation": "Tester", "color": "yellow"},
        "Tit-for-Tat": {"translation": "Wet za Wet", "color": "blue"},
        "Forgiving Tit-for-Tat": {"translation": "Wybaczający Wet za Wet", "color": "brown"},
        "Defector": {"translation": "Zdrajca", "color": "pink"}
    }

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

    # Create handles and labels in sorted order for the legend
    handles = []
    labels = []

    # Sort strategies by their translated names for consistent legend order
    sorted_strategies = sorted(all_strategies, key=lambda s: strategy_info.get(s, {}).get("translation", s))

    for strategy in sorted_strategies:
        translated_strategy = strategy_info.get(strategy, {}).get("translation", strategy)
        color = strategy_info.get(strategy, {}).get("color", "gray") # Default to gray if no color specified
        line, = plt.plot(rounds, plot_data[strategy], marker='o', label=translated_strategy, color=color)
        handles.append(line)
        labels.append(translated_strategy)

    plt.xlabel("Numer Rundy")
    plt.ylabel("Liczba Graczy")
    plt.title("Ilość Graczy na Strategię w Trakcie Poszczególnych Rund")
    plt.xticks(rounds) # Ensure all round numbers are shown on x-axis
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(handles, labels, title="Strategia", loc='center right', bbox_to_anchor=(1, 0.5))
    plt.tight_layout()
    plt.show()

# --- Example Usage ---
# Run on all files with *states*.json in the current directory

# with open('large-states.json') as f:
#       sample_states_data = json.load(f)['states']

# print(f"Number of players: {len(sample_states_data[0])}")

# visualize_strategies_per_round(sample_states_data)

for filename in filter(lambda x: x.rfind("states") != -1, os.listdir()):
    with open(filename) as f:
        states_data = json.load(f)['states']
        visualize_strategies_per_round(states_data)

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

    # Translations for strategy names
    strategy_translations = {
        "Alternator": "Alternator",
        "Random": "Losowy",
        "Cooperator": "Kooperator",
        "Forgiving Tit-for-Tat": "Wybaczający Wet za Wet",
        "Tit-for-Tat": "Wet za Wet",
        "Defector": "Defektor",
        "Suspicious Tit-for-Tat": "Podejrzliwy Wet za Wet",
        "Grudger": "Pamiętliwy",
        "Tester": "Tester",
        "Harrington": "Harrington"
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

    # Get translated strategy names and sort them alphabetically
    sorted_strategies_for_legend = sorted([strategy_translations.get(s, s) for s in all_strategies])

    # Create handles and labels in sorted order for the legend
    handles = []
    labels = []

    for strategy in sorted(all_strategies, key=lambda s: strategy_translations.get(s, s)):
        translated_strategy = strategy_translations.get(strategy, strategy)
        line, = plt.plot(rounds, plot_data[strategy], marker='o', label=translated_strategy)
        handles.append(line)
        labels.append(translated_strategy)

    plt.xlabel("Numer Rundy")
    plt.ylabel("Liczba Graczy")
    plt.title("Ilość Graczy na Strategię w Trakcie Poszczególnych Rund")
    plt.xticks(rounds) # Ensure all round numbers are shown on x-axis
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(handles, labels, title="Strategia") # Use the sorted handles and labels
    plt.tight_layout()
    plt.show()

# --- Example Usage ---
# Run on all files with *states*.json in the current directory

# with open('large-states.json') as f:
#     sample_states_data = json.load(f)['states']

# print(f"Number of players: {len(sample_states_data[0])}")

# visualize_strategies_per_round(sample_states_data)

for filename in filter(lambda x: x.rfind("states") != -1, os.listdir()):
    with open(filename) as f:
        states_data = json.load(f)['states']
        visualize_strategies_per_round(states_data)

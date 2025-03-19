import axelrod as axl
import multiprocessing

# Use a list of strategy instances instead of the strategies themselves
players = [s() for s in axl.strategies]  # Create instances of each strategy
tournament = axl.Tournament(players, turns=10, repetitions=3)
results = tournament.play(processes=0)

output_filename = 'tournament_results.csv'

results.write_summary(output_filename)

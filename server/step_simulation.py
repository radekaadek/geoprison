import re
import matplotlib.pyplot as plt
import os

def plot_simulation_times(log_file_paths):
    """
    Reads multiple log files, extracts simulation times, and plots them as a line chart.
    Each file's data is plotted as a separate line, labeled by its filename.

    Args:
        log_file_paths (list): A list of paths to the log files.
    """
    if not log_file_paths:
        print("No log files provided.")
        return

    plt.figure(figsize=(14, 7)) # Adjust figure size for better readability with multiple lines

    # Iterate through each log file provided
    for log_file_path in log_file_paths:
        times = []
        step_numbers = []
        step_count = 0
        file_name = os.path.basename(log_file_path).replace('.txt', '') # Get name for label

        try:
            with open(log_file_path, 'r') as f:
                for line in f:
                    # Use a regular expression to find lines containing "Game step simulation completed in X.XX seconds."
                    match = re.search(r"Game step simulation completed in (\d+\.\d+) seconds\.", line)
                    if match:
                        try:
                            time_taken = float(match.group(1))
                            times.append(time_taken)
                            step_count += 1
                            step_numbers.append(step_count)
                        except ValueError:
                            print(f"Warning: Could not convert '{match.group(1)}' to a float in file '{file_name}'. Skipping this line.")
        except FileNotFoundError:
            print(f"Error: Log file not found at '{log_file_path}'. Skipping this file.")
            continue # Move to the next file if one is not found
        except Exception as e:
            print(f"An unexpected error occurred while reading '{log_file_path}': {e}. Skipping this file.")
            continue # Move to the next file on other errors

        if not times:
            print(f"No simulation completion times found in log file: '{file_name}'. Skipping this file.")
            continue

        # Plot data for the current log file
        plt.plot(step_numbers, times, marker='o', linestyle='-', label=file_name)

    if not plt.gca().lines: # Check if any lines were actually plotted
        print("No data was successfully plotted from any of the provided log files.")
        return

    # Add titles and labels
    # plt.title('Time Taken for Each Simulation Step Across Multiple Logs')
    plt.title('Czas Trwania Obliczeń Rundy Symulacji')
    # plt.xlabel('Step Number')
    plt.xlabel('Numer Tury')
    # plt.ylabel('Time Taken (seconds)')
    plt.ylabel('Czas Trwania Tury (sekundy)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(title="Konfiguracja") # Add a legend to differentiate lines
    # Show every 4 steps on the x-axis
    all_steps = [step for line in plt.gca().lines for step in line.get_xdata()]
    if all_steps:
        max_step = int(max(all_steps))
        plt.xticks(range(0, max_step + 1, 4))


    # Improve layout and save the plot
    plt.tight_layout()
    output_filename = 'simulation_time_chart_combined.png'
    plt.savefig(output_filename)
    print(f"Combined line chart saved as '{output_filename}'")

if __name__ == "__main__":
    # Get directory path from user
    log_directory_input = input("Please enter the path to the directory containing your log files: ")
    
    log_files_to_process = []
    if os.path.isdir(log_directory_input):
        for filename in os.listdir(log_directory_input):
            if filename.endswith(".txt"):
                full_path = os.path.join(log_directory_input, filename)
                log_files_to_process.append(full_path)
        
        if not log_files_to_process:
            print(f"No .txt log files found in the directory: {log_directory_input}")
    else:
        print(f"Error: The provided path '{log_directory_input}' is not a valid directory.")

    # make the first word an integer and sort the list by that, then sort alphabetically if the numbers are the same
    log_files_to_process.sort(key=lambda x: int(x.split('/')[1].split()[0]))
    plot_simulation_times(log_files_to_process)


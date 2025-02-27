import subprocess
import os

# Get the directory where your script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

scripts = [
    "ranking.py",
    "channel.py",
    "findplaylist.py",
    "playlist.py",
    "combine.py"
]

for script in scripts:
    script_path = os.path.join(SCRIPT_DIR, script)
    print(f"Running {script}...")
    # Use the same Python interpreter that's running this script
    subprocess.run(["python3", script_path], check=True)
    print(f"Finished running {script}\n")

print("All scripts have been executed.")
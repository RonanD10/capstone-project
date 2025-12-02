# from scripts.run_etl import main

import subprocess
import sys

def main():
    subprocess.call([
        sys.executable, "-m", "streamlit", "run", "src/streamlit/app.py"
    ])

main()


    

import subprocess
import sys
from scripts.run_etl import main_etl

def main():
    main_etl()
    subprocess.call([
        sys.executable, "-m", "streamlit", "run", "src/streamlit/app.py"
    ])
  
if __name__ == "__main__":
    main()

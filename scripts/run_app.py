import subprocess
import sys


def main():
    subprocess.call([
        sys.executable, "-m", "streamlit", "run", "src/streamlit/app.py"
    ])
    # run_etl()


if __name__ == "__main__":
    main()


    

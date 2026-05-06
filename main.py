if __name__ == "__main__":
    import subprocess
    import os
    
    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Set PYTHONPATH to include the project root
    env = os.environ.copy()
    env["PYTHONPATH"] = current_dir + os.pathsep + env.get("PYTHONPATH", "")
    
    # Run the streamlit app from the root
    subprocess.run(["streamlit", "run", "src/ui/app.py"], env=env)

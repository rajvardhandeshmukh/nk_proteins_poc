import os
import sys

# Add current directory to path
sys.path.append(os.getcwd())

from models import load_all

try:
    print("Testing load_all()...")
    data = load_all()
    print("Success! Data loaded for all modules:")
    for key in data:
        print(f" - {key}: {type(data[key])}")
except Exception as e:
    print(f"Error during load_all(): {e}")
    import traceback
    traceback.print_exc()

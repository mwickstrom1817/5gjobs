
import os
from google import genai
from google.genai import types

def get_api_key():
    # Try getting from Env
    return os.getenv("GEMINI_API_KEY") or os.getenv("API_KEY")

def debug():
    api_key = get_api_key()
    if not api_key:
        print("No API Key found in env.")
        return

    print(f"Using API Key: {api_key[:5]}...")
    client = genai.Client(api_key=api_key)

    print("\n--- Listing Models ---")
    try:
        models = list(client.models.list())
        for m in models:
            print(f"Name: {m.name}, Methods: {m.supported_generation_methods}")
    except Exception as e:
        print(f"Error listing models: {e}")

    print("\n--- Testing Generate Content (gemini-1.5-flash) ---")
    try:
        response = client.models.generate_content(model='gemini-1.5-flash', contents='Hello')
        print("Success with 'gemini-1.5-flash'")
        print(response.text)
    except Exception as e:
        print(f"Error with 'gemini-1.5-flash': {e}")

    print("\n--- Testing Generate Content (models/gemini-1.5-flash) ---")
    try:
        response = client.models.generate_content(model='models/gemini-1.5-flash', contents='Hello')
        print("Success with 'models/gemini-1.5-flash'")
        print(response.text)
    except Exception as e:
        print(f"Error with 'models/gemini-1.5-flash': {e}")

if __name__ == "__main__":
    debug()

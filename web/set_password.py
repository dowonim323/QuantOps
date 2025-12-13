import os
from pathlib import Path
from werkzeug.security import generate_password_hash
from dotenv import set_key

# Define path to .env file
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / '.env'

def set_password():
    print("=== Dashboard Password Setup ===")
    password = input("Enter new password: ").strip()
    
    if not password:
        print("Error: Password cannot be empty.")
        return

    confirm = input("Confirm new password: ").strip()
    
    if password != confirm:
        print("Error: Passwords do not match.")
        return

    # Hash the password
    password_hash = generate_password_hash(password)
    
    # Create .env if it doesn't exist
    if not ENV_PATH.exists():
        ENV_PATH.touch()

    # Save to .env
    set_key(ENV_PATH, "DASHBOARD_PASSWORD_HASH", password_hash)
    
    print(f"\nSuccess! Password has been updated in {ENV_PATH}")
    print("Please restart the web server to apply changes:")
    print("  docker compose restart web")

if __name__ == "__main__":
    set_password()

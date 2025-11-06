"""Helper script to create .env file from template."""
import os

def create_env_file():
    """Create .env file from template if it doesn't exist."""
    env_example = """# ExchangeRate.host API Configuration
EXCHANGERATE_API_KEY=cbf4d09c53b4d9a283362efa1895d665
EXCHANGERATE_BASE_URL=https://api.exchangerate.host

# Supabase Configuration
SUPABASE_URL=your_supabase_url_here
SUPABASE_KEY=your_supabase_key_here

# Pipeline Configuration
LOG_LEVEL=INFO
BATCH_SIZE=100
"""
    
    if os.path.exists('.env'):
        print(".env file already exists. Skipping creation.")
        return
    
    with open('.env', 'w') as f:
        f.write(env_example)
    
    print(".env file created successfully!")
    print("Please update SUPABASE_URL and SUPABASE_KEY if you want to use Supabase.")

if __name__ == "__main__":
    create_env_file()


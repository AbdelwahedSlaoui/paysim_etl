from pathlib import Path
import yaml

def load_config(env: str = "dev") -> dict:
    """Load environment-specific configuration."""
    config_path = Path("config") / f"{env}.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)
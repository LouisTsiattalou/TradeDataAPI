from pathlib import Path
import yaml

def read_credentials(path_to_file: str = "conf/credentials.yml") -> dict:
    """Read Credentials in yaml format."""
    credentials = yaml.safe_load(Path(path_to_file).read_text())
    return(credentials)

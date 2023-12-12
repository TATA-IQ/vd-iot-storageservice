import yaml
from yaml.loader import SafeLoader


class Config:
    """
    This class reads YAML file from config folder
    """

    def yamlconfig(path: str):
        """
        Args:
            path (str): path of the config file

        Returns:
            dict: configuration from the file
        """
        with open(path, "r") as f:
            data = list(yaml.load_all(f, Loader=SafeLoader))
            print(data)
        return data

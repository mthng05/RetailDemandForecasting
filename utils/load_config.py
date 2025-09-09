import yaml 

def load_cfg(path: str) -> dict:
    with open (path, 'r') as file:
        try :
            cfg = yaml.safe_load(file)
        except yaml.YAMLError as e:
            print(e)
    return cfg
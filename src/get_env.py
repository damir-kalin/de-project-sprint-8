import os

class GetEnv:
    def __init__(self)->None:
        self.path = os.path.dirname(os.path.abspath(__file__)) + '/.env'

    def get_env(self, env_name:str)->str:
        envs = {}
        with open(self.path, 'r') as f:
            envs = {x.rstrip().split('=')[0]:x.rstrip().split('=')[1] for x in f.readlines()}
        return envs[env_name]
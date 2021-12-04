class Configuration:
    def __init__(self, json_obj):
        self.base_url = json_obj['base_url']
        self.db_uri = json_obj['db_uri']
        self.base_path = json_obj['base_path']
        self.signing_key_path = json_obj['signing_key_path']

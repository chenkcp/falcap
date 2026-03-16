class ResultsConfig:
    def __init__(self, config):
        self._save_result = config.get("save_result", False)
        if self._save_result:
            self._bucket = config["bucket"]
            self._region = config["region"]
            self._path = config["bucket_path"]

    @property
    def save_result(self):
        return self._save_result

    @property
    def path(self):
        return self._path

    @property
    def bucket(self):
        return self._bucket

    @property
    def region(self):
        return self._region

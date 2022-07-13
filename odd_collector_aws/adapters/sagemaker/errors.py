class UnknownArtifactTypeError(Exception):
    def __init__(self, name: str, uri: str):
        super().__init__(f"Could not create artifact for {name}, {uri}")

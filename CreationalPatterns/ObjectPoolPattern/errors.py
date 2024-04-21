class PoolResourceError(Exception):
    def __init__(self, *args, **kwargs):
        self.msg: str = f"Some error occurred with object in pool"

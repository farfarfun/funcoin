
class BaseModel:
    def __init__(self, name='base', *args, **kwargs):
        self.name = name

    def train(self, df, *args, **kwargs):
        pass

    def predict(self, df, *args, **kwargs):
        pass

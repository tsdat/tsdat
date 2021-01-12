class AttributeDefinition:
    def __init__(self, name: str, value: str):
        self.name: str = name
        self.value = value
    
    def to_dict(self):
        return {self.name, self.value}
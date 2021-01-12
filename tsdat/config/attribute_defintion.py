class AttributeDefinition:
    def __init__(self, name: str, value: str):
        self.name: str = name
        self.value = value if value is not None else ""
    
    def to_dict(self):
        return {self.name, self.value}

class Node: 
    def evaluate(self):
        pass


class VariableAssignment(Node):
    def __init__(self, name, value) -> None:
        self.name = name
        self.value = value
    def evaluate(self):
        return super().evaluate()

class VariableDeclaration(Node):
    def __init__(self, type, name, value) -> None:
        self.type = type
        self.name = name
        self.value = value
    def evaluate(self):
        return super().evaluate()
        
class GroupOp(Node):
    def __init__(self, registers, collection) -> None:
        self.registers = registers
        self.collection = collection
    def evaluate(self):
        pass
class FilterOp(Node):
    def __init__(self, registers, predicates) -> None:
        self.registers = registers
        self.predicates = predicates
    def evaluate(self):
        pass
    
class Users(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        pass

class Towers(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        pass
    
class Count(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        pass
    
class AllRegisters(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self):
        pass
    
class ProvincesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self):
        pass
    
class MunicipalitiesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self):
        pass

class Predicate(Node):
    def evaluate(self):
        pass

class TimePredicate(Predicate):
    def __init__(self, start_date, end_date) -> None:
        self.start_date = start_date
        self.end_date = end_date
    def evaluate(self):
        pass
        
class LocationPredicate(Predicate):
    def __init__(self, location) -> None:
        self.location = location
    def evaluate(self):
        pass
        
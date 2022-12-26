
class Expression: 
    def evaluate(self):
        pass

class GroupOp(Expression):
    def __init__(self, registers, collection) -> None:
        self.registers = registers
        self.collection = collection
    def evaluate(self):
        pass
class FilterOp(Expression):
    def __init__(self, registers, predicates) -> None:
        self.registers = registers
        self.predicates = predicates
    def evaluate(self):
        pass
    
class Users(Expression):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        pass

class Towers(Expression):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        pass
    
class Count(Expression):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        pass
    
class AllRegisters(Expression):
    def __init__(self) -> None:
        pass
    def evaluate(self):
        pass
    
class ProvincesCollection(Expression):
    def __init__(self) -> None:
        pass
    def evaluate(self):
        pass
    
class MunicipalitiesCollection(Expression):
    def __init__(self) -> None:
        pass
    def evaluate(self):
        pass

class Predicate(Expression):
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
        
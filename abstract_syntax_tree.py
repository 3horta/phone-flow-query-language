from abc import ABC, abstractmethod
from atexit import register
from lang.context import Context
from lang.type import Instance, TimeInterval
from lang.type import Type
from api.pfql_api import *

class Node(ABC):
    @abstractmethod
    def __init__(self) -> None:
        self.computed_type = None
        
    @abstractmethod
    def evaluate(self, context: Context):
        pass


class VariableCall(Node):
    def __init__(self, name: str) -> None:
        self.name = name
    
    def evaluate(self, context: Context):
        context.resolve(self.name)
        
    
class VariableAssignment(Node):
    def __init__(self, name, value) -> None:
        self.name = name
        self.value = value
    def evaluate(self, context: Context):
        variable = context.resolve(self.name)
        if variable:
            variable.value = self.value.evaluate(context)
        else:
            raise Exception('Not defined variable.')

class VariableDeclaration(Node):
    def __init__(self, type, name, value) -> None:
        self.type = type
        self.name = name
        self.value = value
    def evaluate(self, context: Context):
        variable = context.resolve(self.name)
        if not variable:
            context.define(self.name, Instance(Type.get(self.type), self.value.evaluate(context)))
        else:
            raise Exception(f"Defined variable '{self.name}'.")
        
class GroupOp(Node):
    def __init__(self, registers, collection) -> None:
        self.registers = registers
        self.collection = collection
    def evaluate(self, context: Context):
        pass # method from pfql_api.py
        
class FilterOp(Node):
    def __init__(self, registers, predicates) -> None:
        self.registers = registers
        self.predicates = predicates
    def evaluate(self, context: Context):
        pass # method from pfql_api.py
    
class Users(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        get_users_columns(self.registers)

class Towers(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        get_towers_columns(self.registers)
    
class Count(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self):
        count(self.registers)
    
class AllRegisters(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self):
        charge_data()
    
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
    pass

class TimePredicate(Predicate):
    def __init__(self, start_date, end_date) -> None:
        self.start_date = start_date
        self.end_date = end_date
    def evaluate(self, context: Context):
        return TimeInterval(self.start_date, self.end_date)

class LocationPredicate(Predicate):
    def __init__(self, location) -> None:
        self.location = location
    def evaluate(self, context: Context):
        return self.location
import datetime
from abc import ABC, abstractmethod
from calendar import monthrange
from typing import List

from api.pfql_api import TimeInterval
from lang.context import Context
from lang.type import FunctionInstance, Instance, Type


class Node(ABC):
    @abstractmethod
    def __init__(self) -> None:
        self.computed_type = None
        
    @abstractmethod
    def evaluate(self, context: Context):
        pass
    
class Program(Node):
    def __init__(self, statements: List[Node]) -> None:
        self.statements = statements
    def evaluate(self, context: Context):
        for statement in self.statements:
            statement.evaluate(context)


class FunctionDeclaration(Node):
    def __init__(self, type, name, parameters, body) -> None:
        self.type = type
        self.name = name
        self.parameters = parameters
        self.body = body
        
    def evaluate(self, context: Context):
        child_context: Context = context.make_child()
        for parameter in self.parameters:
            child_context.define(parameter[1], Instance(Type.get(parameter[0]), None))
        context.define(self.name, FunctionInstance(child_context, self.type, self.body))
        
class ReturnStatement(Node):
    def __init__(self, expression) -> None:
        self.expression = expression
    def evaluate(self, context: Context):
        return self.expression.evaluate(context)


class VariableCall(Node):
    def __init__(self, name: str) -> None:
        self.name = name
    
    def evaluate(self, context: Context):
        return context.resolve(self.name).value
        
    
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
    def evaluate(self, context: Context):
        pass # method from pfql_api.py

class Towers(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        pass # method from pfql_api.py
    
class Count(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        pass # method from pfql_api.py
    
class AllRegisters(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        pass # from pfql_api.py
    
class ProvincesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        pass # from pfql_api.py
    
class MunicipalitiesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        pass # from pfql_api.py

class Predicate(Node):
    pass

class TimePredicate(Predicate):
    def __init__(self, start_date, end_date) -> None:
        self.start_date = self.build_start_date(start_date)
        self.end_date = self.build_end_date(end_date)
    def evaluate(self, context: Context):
        return TimeInterval(self.start_date, self.end_date)
    def build_start_date(self, date: str):
        splitted_date_str = date.split('-')
        splitted_date = [int(item) for item in splitted_date_str]
        if len(splitted_date) == 1:
            return datetime.date(splitted_date[0], 1, 1)
        if len(splitted_date) == 2:
            return datetime.date(splitted_date[1], splitted_date[0], 1)
        return datetime.date(splitted_date[2], splitted_date[1], splitted_date[0])
    def build_end_date(self, date: str):
        splitted_date_str = date.split('-')
        splitted_date = [int(item) for item in splitted_date_str]
        if len(splitted_date) == 1:
            return datetime.date(splitted_date[0], 12, 31)
        if len(splitted_date) == 2:
            return datetime.date(splitted_date[1], splitted_date[0], monthrange(splitted_date[1], splitted_date[0])[1])
        return datetime.date(splitted_date[2], splitted_date[1], splitted_date[0])

class LocationPredicate(Predicate):
    def __init__(self, location) -> None:
        self.location = location
    def evaluate(self, context: Context):
        return self.location
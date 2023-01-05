import datetime
import operator
from abc import ABC, abstractmethod
from calendar import monthrange
from typing import List

from api.pfql_api import TimeInterval
from lang.context import Context
from lang.type import FunctionInstance, Instance, Type

OPERATORS = {'>' : operator.gt, '<': operator.lt, '==': operator.eq, '>=': operator.ge, '<=': operator.le, '+': operator.add, '-': operator.sub}

TOKEN_TO_TYPE = {'BOOL': 'bool', 'NUM': 'int'}

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
            
class ArithmeticOp(Node):
    def __init__(self, left, op, right) -> None:
        self.left = left
        self.op = op
        self.right = right
        
    def evaluate(self, context: Context):
        l = self.left.evaluate(context)
        r = self.right.evaluate(context)
        return OPERATORS[self.op](l, r)
        
            
class Show(Node):
    def __init__(self, item) -> None:
        self.item = item
    def evaluate(self, context: Context):
        value = self.item.evaluate(context)
        print(value)

class Literal(Node):
    def __init__(self, value, type) -> None:
        self.value= value
        self.type= Type.get(TOKEN_TO_TYPE[type])
    def evaluate(self, context: Context):
        return self.value
            
class IfStatement(Node):
    def __init__(self, condition, body) -> None:
        self.condition = condition
        self.body = body
    def evaluate(self, context: Context):
        if not self.condition.evaluate(context):
            return
        child_context: Context = context.make_child()
        for line in self.body:
            if isinstance(line, ReturnStatement):
                return line.evaluate(child_context)
            line.evaluate(child_context)
    
class BinaryComparer(Node):
    def __init__(self, left_expr, comparer, right_expr) -> None:
        self.left_expr = left_expr
        self.comparer = comparer
        self.right_expr = right_expr
        
    def evaluate(self, context: Context):
        eval_left_expr = self.left_expr.evaluate(context)
        eval_right_expr = self.right_expr.evaluate(context)
        return OPERATORS[self.comparer](eval_left_expr, eval_right_expr)

class FunctionCall(Node):
    def __init__(self, name, args) -> None:
        self.name = name
        self.args = args

    def evaluate(self, context: Context):
        function: FunctionInstance = context.resolve(self.name)
        for i in range(len(function.parameters)):
            parameter = function.parameters[i]
            item = self.args[i].evaluate(context)
            function.context.define(parameter[1], Instance(Type.get(parameter[0]), item))
        for line in function.body:
            if isinstance(line, ReturnStatement):
                return line.evaluate(function.context)
            line.evaluate(function.context)
            

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
        context.define(self.name, FunctionInstance(child_context, self.type, self.parameters, self.body))
        
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
        variable= context.resolve(self.name)
        new_value=self.value.evaluate(context)
        variable.value = new_value

class VariableDeclaration(Node):
    def __init__(self, type, name, value) -> None:
        self.type = type
        self.name = name
        self.value = value
    def evaluate(self, context: Context):
        context.define(self.name, Instance(Type.get(self.type), self.value.evaluate(context)))
        
class GroupOp(Node):
    def __init__(self, registers, collection) -> None:
        self.registers = registers
        self.collection = collection
    def evaluate(self, context: Context):
        registers= self.registers.evaluate(context)
        collection=[]
        for c in self.collection:
            collection.append(c.evaluate(context))

        # result = Metodo_group_de_API(registers:DataFrame, collection: List)
        # return result

        
class FilterOp(Node):
    def __init__(self, registers, predicates) -> None:
        self.registers = registers
        self.predicates = predicates
    def evaluate(self, context: Context):
        registers= self.registers.evaluate(context)
        predicates=[]
        for pred in self.predicates:
            predicates.append(pred.evaluate(context))

        # result = Metodo_filter_de_API(registers:DataFrame, predicates: List)
        # return result
    
class Users(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        register = self.registers.evaluate(context)
        # result = Metodo_get_users_de_API(register:DataFrame)


class Towers(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        register = self.registers.evaluate(context)
        # result = Metodo_get_towers_de_API(register:DataFrame)
    
class Count(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        register = self.registers.evaluate(context)
        # result = Metodo_count_de_API(register:DataFrame)
    
class AllRegisters(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        # result = get_ALL_de_API
        pass
    
class ProvincesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        # result = get_Provinces_de_API   Nota: Devuelve Lista de string Ex: ["La Habana", "Cienfuegos",...]
        pass
    
class MunicipalitiesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        # result = get_Municipalities_de_API   Nota: Devuelve Lista de string Ex: ["La Habana.Playa", "Matanzas.Cardenas",...]
        pass

class Predicate(Node):
    pass

class TimePredicate(Predicate):
    def __init__(self, start_date, end_date) -> None:
        self.start_date = self.build_start_date(start_date)
        self.end_date = self.build_end_date(end_date)
    
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

    def evaluate(self, context: Context):
        pass
        # return TimeInterval(self.start_date, self.end_date) 
        # Esta es una estructura que debe estar en la API. Lo q se hace aqui es construirla

class LocationPredicate(Predicate):
    def __init__(self, location) -> None:
        self.location = location
    def evaluate(self, context: Context):
        return self.location
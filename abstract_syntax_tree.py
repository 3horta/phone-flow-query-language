import datetime
import operator
from abc import ABC, abstractmethod
from calendar import monthrange
from typing import List

from api.pfql_api import *
from lang.context import Context
from lang.type import FunctionInstance, Instance, Type

OPERATORS = {'>' : operator.gt, '<': operator.lt, '==': operator.eq, '>=': operator.ge, '<=': operator.le, '+': operator.add, '-': operator.sub}

TOKEN_TO_TYPE = {'BOOL': 'bool', 'NUM': 'int'}

ALL = charge_data()

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
            result=line.evaluate(child_context)
            if result and isinstance(line, IfStatement):
                return result
            
class WhileStatement(Node):
    def __init__(self, condition, body) -> None:
        self.condition = condition
        self.body = body
    def evaluate(self, context: Context):
        
        child_context: Context = context.make_child()
        while self.condition.evaluate(context):
            for line in self.body:
                if isinstance(line, ReturnStatement):
                    return line.evaluate(child_context)
                result=line.evaluate(child_context)
                if result and isinstance(line, IfStatement):
                    return result
    
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
        child_context = function.context.make_child() 
        for i in range(len(function.parameters)):
            parameter = function.parameters[i]
            item = self.args[i].evaluate(context)
            child_context.define(parameter[1],Instance(Type.get(parameter[0]), item))
            #function.context.define(parameter[1], Instance(Type.get(parameter[0]), item))
        for line in function.body:
            if isinstance(line, ReturnStatement):
                return line.evaluate(child_context)
            result=line.evaluate(child_context)
            ret_cond = not isinstance(result,type(None)) and isinstance(line, IfStatement)
            if ret_cond:
                return result
            

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

        result = group_by(registers, collection)
        return result

        
class FilterOp(Node):
    def __init__(self, registers, predicates) -> None:
        self.registers = registers
        self.predicates = predicates
    def evaluate(self, context: Context):
        registers = self.registers.evaluate(context)
        predicates = []
        for pred in self.predicates:
            predicates.append(pred.evaluate(context))
        result = filter(registers, predicates)
        return result
    
class Users(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        register = self.registers.evaluate(context)
        result = get_users_columns(register)
        return result


class Towers(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        register = self.registers.evaluate(context)
        result = get_towers_columns(register)
    
class Count(Node):
    def __init__(self, registers) -> None:
        self.registers = registers
    def evaluate(self, context: Context):
        register = self.registers.evaluate(context)
        result = count(register)
        return result
    
class AllRegisters(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        return ALL
    
class ProvincesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        return PROVINCES
        
class MunicipalitiesCollection(Node):
    def __init__(self) -> None:
        pass
    def evaluate(self, context: Context):
        return MUNICIPALITIES

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
        return TimeInterval(self.start_date, self.end_date) 

class LocationPredicate(Predicate):
    def __init__(self, location: str) -> None:
        self.location = location.replace('"', '')
    def evaluate(self, context: Context):
        return self.location
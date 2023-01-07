from datetime import date
from typing import List
from api.pfql_api import MUNICIPALITIES, PROVINCES

import lang.visitor as visitor
from abstract_syntax_tree import (AllRegisters, ArithmeticOp, BinaryComparer,
                                  Count, FilterOp, FunctionCall,
                                  FunctionDeclaration, GroupOp, IfStatement,
                                  Literal, LocationPredicate,
                                  MunicipalitiesCollection, Program,
                                  ProvincesCollection, ReturnStatement, Show,
                                  TimePredicate, Towers, Users,
                                  VariableAssignment, VariableCall,
                                  VariableDeclaration, WhileStatement)
from lang.context import Context
from lang.type import FunctionInstance, Type


class SemanticChecker:
    def __init__(self, context: Context) -> None:
        self.context = context
        
    @visitor.on("node")
    def visit(self, node):
        pass

    @visitor.when(Program)
    def visit(self, node: Program):
        for statement in node.statements:
            self.visit(statement)
            
    @visitor.when(ArithmeticOp)
    def visit(self, node: ArithmeticOp):
        self.visit(node.left)
        self.visit(node.right)
        
        valid_types_str = ['int', 'registerset', 'clusterset', 'list(string)', 'string']
        valid_types = []
        for item in valid_types_str:
            valid_types.append(Type.get(item))
        
        if node.left.computed_type != node.right.computed_type:
            raise Exception("Arithmetic operands must be the same type.")
        if node.right.computed_type not in valid_types:
            raise Exception("Invalid operands.")
        node.computed_type = node.left.computed_type
            
    @visitor.when(Show)
    def visit(self, node: Show):
        self.visit(node.item)
            
    @visitor.when(Literal)
    def visit(self, node:Literal):
        node.computed_type = node.type
        
    @visitor.when(IfStatement)
    def visit(self, node: IfStatement):
        self.visit(node.condition)
        if node.condition.computed_type is not Type.get('bool'):
            raise Exception(f"Given condition is not boolean.")
        
        child_context: Context = self.context.make_child()
        child_semantic_checker = SemanticChecker(child_context)
        
        node.computed_type= Type.get('void')
        
        for line in node.body:
            child_semantic_checker.visit(line)
            cond= isinstance(line,ReturnStatement) or isinstance(line,IfStatement) or isinstance(line,WhileStatement)
            if cond:
                if node.computed_type is not Type.get('void') and line.computed_type is not Type.get('void') :
                    if node.computed_type is not line.computed_type:
                        raise Exception('Return type not valid')
                elif line.computed_type is not Type.get('void'):
                    node.computed_type= line.computed_type
                if isinstance(line,ReturnStatement): return
    
        
        
    @visitor.when(WhileStatement)
    def visit(self, node: WhileStatement):
        self.visit(node.condition)
        if node.condition.computed_type is not Type.get('bool'):
            raise Exception(f"Given condition is not boolean.")
        
        node.computed_type = Type.get('void')
        child_context: Context = self.context.make_child()
        child_semantic_checker = SemanticChecker(child_context)
        for line in node.body:
            child_semantic_checker.visit(line)
            cond= isinstance(line,ReturnStatement) or isinstance(line,IfStatement) or isinstance(line,WhileStatement)
            if cond:
                if node.computed_type is not Type.get('void') and line.computed_type is not Type.get('void') :
                    if node.computed_type is not line.computed_type:
                        raise Exception('Return type not valid')
                elif line.computed_type is not Type.get('void'):
                    node.computed_type= line.computed_type
                if isinstance(line,ReturnStatement): return
        
    
    @visitor.when(BinaryComparer)
    def visit(self, node: BinaryComparer):
        self.visit(node.left_expr)
        self.visit(node.right_expr)
        
        if node.left_expr.computed_type == Type.get('void') or node.right_expr.computed_type == Type.get('void'):
            raise Exception(f"{'void'} expression not admissible for comparison.")
        
        if node.left_expr.computed_type != node.right_expr.computed_type:
            raise Exception("Expressions to compare must be the same type.")
        
        if node.comparer in ['>', '<', '>=', '<='] and node.left_expr.computed_type is not Type.get('int'):
            raise Exception(f"Invalid expression type for '{node.comparer}' comparer.")
        
        node.computed_type = Type.get('bool')
            
    @visitor.when(FunctionCall)
    def visit(self, node: FunctionCall):
        function: FunctionInstance = self.context.resolve(node.name)
        if not function: 
            raise Exception(f"Not defined function '{self.name}'.")
        
        if len(node.args) != len(function.parameters):
            raise Exception(f"{len(node.args)} arguments given to {node.name} function, {len(function.parameters)} arguments expected.")
        
        param_index = 0
        for argument in node.args:
            self.visit(argument)
            if argument.computed_type is not Type.get(function.parameters[param_index][0]):
                raise Exception(f"Not expected '{argument.computed_type}' as type of parameter number {param_index + 1}.")
            param_index+=1
        
        node.computed_type = function.type
        
    @visitor.when(FunctionDeclaration)
    def visit(self, node: FunctionDeclaration):
        func = self.context.resolve(node.name)
        if func: 
            raise Exception(f"Defined function '{self.name}'.")
        
        function_type = Type.get(node.type)
        
        child_context: Context = self.context.make_child()
        for parameter in node.parameters:
            child_context.define(parameter[1], Type.get(parameter[0]))
        
        func_instance = FunctionInstance(child_context, function_type, node.parameters, None)
        self.context.define(node.name, func_instance)
        
        child_semantic_checker = SemanticChecker(child_context)
        has_return = False
        for sub_program in node.body:
            child_semantic_checker.visit(sub_program)
            if_ret= isinstance(sub_program, IfStatement) and sub_program.computed_type is not Type.get('void')
            while_ret= isinstance(sub_program, WhileStatement) and sub_program.computed_type
            ret= isinstance(sub_program, ReturnStatement) or if_ret or while_ret
            if ret:
                if not if_ret and not while_ret:
                    has_return = True
                if sub_program.computed_type is not function_type:
                    raise Exception(f"Not expected '{sub_program.computed_type}' as return type.")
        
        if not has_return and function_type != Type.get('void'):
            raise Exception(f"Return statement expected.")
        
        node.computed_type = function_type
        
    
    @visitor.when(ReturnStatement)
    def visit(self, node: ReturnStatement):
        self.visit(node.expression)
        node.computed_type = node.expression.computed_type
        

    @visitor.when(VariableAssignment)
    def visit(self, node: VariableAssignment):
        self.visit(node.value)
        computed_type_node_value = node.value.computed_type
        var_type = self.context.resolve(node.name)
        if var_type is None:
            raise Exception(f"Variable '{node.name}' not defined.")
        if var_type != computed_type_node_value:
            raise Exception(f"Can't assign value {node.value} to variable '{node.name}'. Type '{var_type}' different to '{computed_type_node_value}'.")
        node.computed_type = var_type
    
    @visitor.when(VariableDeclaration)
    def visit(self, node: VariableDeclaration):
        var_type = Type.get(node.type)
        if node.name in self.context.symbols.keys():
            raise Exception(f"Defined variable '{node.name}'.")
        else:
            self.context.define(node.name, var_type)
        self.visit(node.value)
        if node.value.computed_type is not var_type:
            raise Exception(f"{node.value.computed_type} not expected.")
        node.computed_type = var_type
        
    @visitor.when(GroupOp)
    def visit(self, node: GroupOp):
        self.visit(node.registers)
        if node.registers.computed_type is not Type.get('registerset'):
            raise Exception(f"{node.registers.computed_type} not expected.")
        for item in node.collection:
            self.visit(item)
            if item.computed_type is not Type.get('list(string)'):
                raise Exception(f"{item.computed_type} not expected.")
        node.computed_type = Type.get('clusterset')
    
    @visitor.when(VariableCall)
    def visit(self, node: VariableCall):
        var_type = self.context.resolve(node.name)
        if var_type is None:
            raise Exception(f"Variable '{node.name}' not defined.")
        node.computed_type = var_type
            
    @visitor.when(ProvincesCollection)
    def visit(self, node: ProvincesCollection): 
        node.computed_type = Type.get('list(string)')
        
    @visitor.when(MunicipalitiesCollection)
    def visit(self, node: MunicipalitiesCollection): 
        node.computed_type = Type.get('list(string)')
    
    @visitor.when(FilterOp)
    def visit(self, node: FilterOp): 
        self.visit(node.registers)
        if node.registers.computed_type is not Type.get('registerset'):
            raise Exception(f"{node.registers.computed_type} not expected.")
        for predicate in node.predicates:
            self.visit(predicate)
            if predicate.computed_type is not Type.get('string') and predicate.computed_type is not Type.get('time_interval'):
                raise Exception(f"{predicate.computed_type} not expected.")
        node.computed_type = Type.get('registerset')
    
    @visitor.when(Users)
    def visit(self, node: Users):
        self.visit(node.registers)
        if node.registers.computed_type is not Type.get('registerset'):
            raise Exception(f"'{node.registers.computed_type}' type not expected.")
        node.computed_type = Type.get('list(string)')
        
    @visitor.when(Towers)
    def visit(self, node: Towers):
        self.visit(node.registers)
        if node.registers.computed_type is not Type.get('registerset'):
            raise Exception(f"'{node.registers.computed_type}' type not expected.")
        node.computed_type = Type.get('list(string)')
    
    @visitor.when(AllRegisters)
    def visit(self, node: AllRegisters): 
        node.computed_type = Type.get('registerset')
        
    @visitor.when(Count)
    def visit(self, node: Count): 
        node.computed_type = Type.get('int')
    
    
    @visitor.when(TimePredicate)
    def visit(self, node: TimePredicate): 
        if not isinstance(node.start_date, date):
            raise Exception(f"{node.start_date} is not a valid date.")
        if not isinstance(node.end_date, date):
            raise Exception(f"{node.end_date} is not a valid date.")
        node.computed_type = Type.get('time_interval')
        
    
    @visitor.when(LocationPredicate)
    def visit(self, node: LocationPredicate):
        if not isinstance(node.location, str):
            raise Exception(f"{node.location} is not a valid location.")
        province_municipality: List[str] = node.location.split('.')
        
        if province_municipality[0] in PROVINCES:
            if len(province_municipality) == 1 or node.location in MUNICIPALITIES:
                node.computed_type = Type.get('string')
                return
        raise Exception(f"{node.location} is not a valid location.")
        
    
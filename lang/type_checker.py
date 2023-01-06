from typing import List
from abstract_syntax_tree import AllRegisters, Count, FilterOp, GroupOp, LocationPredicate, MunicipalitiesCollection, Node, Program, ProvincesCollection, TimePredicate, Towers, Users, VariableAssignment, VariableCall, VariableDeclaration
#from api.pfql_api import LOCATIONS
from lang.context import Context
from lang.type import Type
import lang.visitor as visitor
from datetime import date

class TypeChecker:
    def __init__(self, context: Context) -> None:
        self.context = context
        
    @visitor.on("node")
    def visit(self, node):
        pass

    @visitor.when(Program)
    def visit(self, node: Program):
        for statement in node.statements:
            self.visit(statement)

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
            if item.computed_type is not Type.get('str_list'):
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
        node.computed_type = Type.get('str_list')
        
    @visitor.when(MunicipalitiesCollection)
    def visit(self, node: MunicipalitiesCollection): 
        node.computed_type = Type.get('str_list')
    
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
            raise Exception(f"{node.registers.computed_type} not expected.")
        node.computed_type = Type.get('str_list')
        
    @visitor.when(Towers)
    def visit(self, node: Towers):
        self.visit(node.registers)
        if node.registers.computed_type is not Type.get('registerset'):
            raise Exception(f"{node.registers.computed_type} not expected.")
        node.computed_type = Type.get('str_list')
    
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
        #if province_municipality[0] in LOCATIONS.keys():
        #    if len(province_municipality) == 1 or province_municipality[1] in LOCATIONS[province_municipality[0]]:
        #        node.computed_type = Type.get('string')
        #        return
        #raise Exception(f"{node.location} is not a valid location.")
        
    
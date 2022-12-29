from typing import List
from abstract_syntax_tree import Count, FilterOp, GroupOp, LocationPredicate, Node, TimePredicate, Towers, Users, VariableAssignment, VariableDeclaration
from api.pfql_api import LOCATIONS
from lang.context import Context
from lang.type import Type
import lang.visitor as visitor
from datetime import date


class TypeChecker:
    def __init__(self, context: Context) -> None:
        self.context = Context

    @visitor.when(VariableAssignment)
    def visit(self, node): 
        pass
    
    @visitor.when(VariableDeclaration)
    def visit(self, node): 
        pass
    
    @visitor.when(GroupOp)
    def visit(self, node): 
        pass
    
    @visitor.when(FilterOp)
    def visit(self, node): 
        pass
    
    @visitor.when(Users, Towers)
    def visit(self, node):
        self.visit(node.registers)
        if node.registers.computed_type is not Type.get('registerset'):
            raise Exception(f"{node.registers.computed_type} not expected.")
        
        node.computed_type = Type.get('list(string)')
    
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
        if province_municipality[0] in LOCATIONS.keys():
            if len(province_municipality) == 1 or province_municipality[1] in LOCATIONS[province_municipality[0]]:
                node.computed_type = Type.get('string')
                return
        raise Exception(f"{node.location} is not a valid location.")
        
    
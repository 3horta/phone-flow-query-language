from abstract_syntax_tree import FilterOp
from context.context import Context
import context.visitor as visitor


class TypeChecker:
    def __init__(self, context: Context) -> None:
        self.context = Context

    @visitor.when(FilterOp)
    def visit(self, node): 
        pass
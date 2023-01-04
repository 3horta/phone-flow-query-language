from lang.type_checker import TypeChecker
from parsing import parser
from lang.context import Context
# Parse an expression
ast = parser.parse(
    '''
    registerset a = filter ALL by time ( 01-03-2021 ); 
    
    
    
    
    
    
    
    '''
    )

type_checker = TypeChecker(Context())
type_checker.visit(ast)
ast.evaluate(Context())
print(ast)
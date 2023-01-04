from lang.type_checker import TypeChecker
from parsing import parser
from lang.context import Context
# Parse an expression
ast = parser.parse(
    '''
    registerset a = filter ALL by time ( 2021-03-01 );
    
    
    
    
    
    
    '''
    )

type_checker = TypeChecker(Context())
type_checker.visit(ast)
ast.evaluate(Context())
print(ast)
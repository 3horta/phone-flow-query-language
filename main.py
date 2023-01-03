from lang.type_checker import TypeChecker
from parsing import parser
from lang.context import Context
# Parse an expression
ast = parser.parse(
    '''
    registerset a = filter ALL by time ( 1-12-3988 , 7-8-9878 ); 
    clusterset b = group a by {PROVINCES};
    
    
    
    
    
    
    
    '''
    )

type_checker = TypeChecker(Context())
type_checker.visit(ast)
ast.evaluate(Context())
print(ast)
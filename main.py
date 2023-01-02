from lang.semantic_checker import SemanticChecker
from parsing import parser
from lang.context import Context
# Parse an expression
ast = parser.parse(
    '''
    #registerset a = filter ALL by time ( 1-12-3988 , 7-8-9878 ), time(1200, 1209); 
    #clusterset b = group a by {MUNICIPALITIES};
    #list(string) c = users(a);
    #list(string) d = towers(filter filter a by time(1200, 1230) by time(0111, 1000));
    #int number = count(a);
    #a = filter a by time(1200, 1230);
    
    
    function list(string) testing_func(registerset z) {
        return users(z);
    };
    
    
    
    
    '''
    )

type_checker = SemanticChecker(Context())
type_checker.visit(ast)
ast.evaluate(Context())
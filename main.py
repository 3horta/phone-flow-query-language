from lang.semantic_checker import SemanticChecker
from parsing import parser
from lang.context import Context
# Parse an expression
ast = parser.parse(
    '''
    registerset a = filter ALL by time ( 1-3-2021, 3-4-2022 ); 
    #clusterset b = group a by {MUNICIPALITIES};
    #list(string) c = users(a);
    #list(string) d = towers(filter filter a by time(1200, 1230) by time(0111, 1000));
    #int number = count(a);
    #filter ALL by time(1200, 1230);
    
    
    function int pepe(int a, int b) {
        list(string) d = users(ALL);
        return count(ALL);
    };
    
    pepe(count(ALL), count(a));
    
    
    
    
    
    '''
    )

type_checker = SemanticChecker(Context())
type_checker.visit(ast)
ast.evaluate(Context())
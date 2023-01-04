from lang.semantic_checker import SemanticChecker
from parsing import parser
from lang.context import Context
# Parse an expression
ast = parser.parse(
    '''
<<<<<<< HEAD
    registerset a = filter ALL by time ( 01-03-2021 ); 
    
    
    
=======
    registerset a = filter ALL by { time ( 1-12-3988 , 7-8-9878 ), time(1200, 1209) }; 
    clusterset b = group a by { MUNICIPALITIES, PROVINCES };
    #list(string) c = users(a);
    #list(string) d = towers(filter filter a by time(1200, 1230) by time(0111, 1000));
    #int number = count(a);

    #filter ALL by {location("Matanzas")};
    list(string) d = users(ALL);
>>>>>>> dev
    
    function int pepe(int a, int b) {
        list(string) d = users(ALL);
        if (users(ALL) == d) {
            return d;
        };
        return count(ALL);
    };
    
    int sol = pepe(count(ALL), count(ALL));
    
    if (count(ALL) == count(ALL)) {
        int a = count(ALL);
        if (users(ALL) == d) {
            return d;
        };
    };
    
    bool i = false;
    int j = 5;
    list(string) k = PROVINCES ;
    list(string) h = MUNICIPALITIES;
    group ALL by {k};
    '''
)

type_checker = SemanticChecker(Context())
type_checker.visit(ast)
ast.evaluate(Context())
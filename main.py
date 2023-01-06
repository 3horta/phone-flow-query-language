from lang.context import Context
from lang.semantic_checker import SemanticChecker
from parsing import parser

# Parse an expression
ast = parser.parse(
    '''
    # registerset a = filter ALL by { time ( 1-12-3988 , 7-8-9878 ), time(1200, 1209) }; 
    # clusterset b = group a by { MUNICIPALITIES, PROVINCES };
    # list(string) c = users(a);
    # list(string) tw= towers(ALL);
    # registerset rg = filter filter a by {time(1200, 1230)} by {time(0111, 1000)};
    # list(string) d = towers(rg);
    # # int number = count(a);

    
    # d = users(ALL);
    
    # function int pepe(int a, int b) {
    #    list(string) d = users(ALL);
    
    #   if (users(ALL) == d) {
    #      return d;
    #    };
    #     return count(ALL);
    # };
    
    # int sol = pepe(count(ALL), count(ALL));
    
    
    # if (count(ALL) == count(ALL)) {
    #     int a = count(ALL);
    #     if (users(ALL) == d) {
    #         return d;
    #     };

    # };
    
    # bool i = false;
    # int j = 5;
    # list(string) k = PROVINCES ;
    # list(string) h = MUNICIPALITIES;
    # group ALL by {k};

    # function int sum(int a, int b) {
    #     return a + b;
    # };
    
    # show(sum(1, 2));
    
    # int c = 1 + 2;
    # show(c);
    
    function int Fibonacci(int n) {
        if (n == 1) {
            return 0;
        };
        if (n == 2) {
            return 1;
        };
        return Fibonacci(n - 1) + Fibonacci(n - 2);
    };
    
    show(Fibonacci(6));
    
    # int a = 0;
    # while (a < 10) {
    #   show(a);
    #   a = a + 1;
    # };
    
    int n = 6;
    int a = 0;
    int b = 1;
    int result = 0;
    while (n > 1) {
        result = a + b;
        a = b;
        b = result;
        n = n - 1;
    };
    show(a);
    
    # registerset r = filter ALL by { location("La Habana"), location("La Habana.Playa") };
    # registerset fr = filter r by {location("La Habana.Playa")};
    # registerset frd = filter r by {time(2022, 2023)};
    # show(count(ALL));
    # show(users(ALL));
    # show(group ALL by {PROVINCES, MUNICIPALITIES});
    # show(group ALL by {PROVINCES, PROVINCES});
    # show(filter ALL by { time(1-1-2021, 31-12-2021) } + filter ALL by { time(1-1-2022, 31-12-2022) } );
    # show(ALL -  filter by { time(1-1-2022, 31-12-2022) } );
    '''
    )

type_checker = SemanticChecker(Context())
type_checker.visit(ast)
ast.evaluate(Context())
from ply.yacc import yacc

# -----------------------------------------------------------------------------
#   Grammar
#
#   Program        : filter ALL by Predicate_list ;
#   
#   Predicate_list : Predicate, Predicate_list 
#                  | Predicate 
#
#   Predicate      : time ( date , date )
#                  | location ( string )
#
# -----------------------------------------------------------------------------

# Write functions for each grammar rule which is
# specified in the docstring.
def p_filter(p):
    '''
    Program : filter ALL by Predicate_list ;
    '''
    # p is a sequence that represents rule contents.
    #
    #  Program : filter ALL  by    Predicate_list ;
    #   p[0]   : p[1]   p[2] p[3]  p[4]
    # 
    p[0] = ('filterop', p[1], p[2], p[4])

def p_predicate_list(p):
    pass

def p_predicate(p):
    pass

def p_error(p):
    print(f'Syntax error at {p.value!r}')

# Build the parser
parser = yacc()

# Parse an expression
#ast = parser.parse('2 * 3 + 4 * (5 - x)')
#print(ast)
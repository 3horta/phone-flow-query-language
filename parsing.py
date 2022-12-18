from ply.yacc import yacc
from lexer import *
from abstract_syntax_tree import *

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
    Program : FILTER ALL BY Predicate_list END
    '''
    # p is a sequence that represents rule contents.
    #
    #  Program : filter ALL  by    Predicate_list ;
    #   p[0]   : p[1]   p[2] p[3]  p[4]           p[5]
    # 
    p[0] = FilterOp(p[1], p[2], p[4]) # ('filterop', p[1], p[2], p[4])

def p_predicate_list(p):
    '''
    Predicate_list : Predicate COMMA Predicate_list
                   | Predicate
    '''
    if (len(p) == 4):
        p[0] = [Predicate(p[1])].extend(p[3]) # [('predicate', p[1])] + p[3]
    elif (len(p) == 2):
        p[0] = [Predicate(p[1])]
    pass

def p_predicate(p):
    '''
    Predicate  : TIME LPAREN DATE COMMA DATE RPAREN
               | LOCATION LPAREN STRING RPAREN
    '''
    if(p[1] == 'time'):
        p[0]= TimePredicate('time_pred', p[3], p[5])
    elif p[1] =='location':
        p[0]= LocationPredicate('location_pred', p[3])

def p_error(p):
    print(f'Syntax error at {p.value!r}')

# Build the parser
parser = yacc()

# Parse an expression
ast = parser.parse('filter ALL by time(1-12-3988, 7-8-9878) ; ')
print(ast)
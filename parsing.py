from ply.yacc import yacc
from lexer import *
from abstract_syntax_tree import *

# -----------------------------------------------------------------------------
#   Grammar
#
#   Program          : group Register_set by Collection_list; 
#                    | users ( Register_set ) ;
#                    | towers ( Register_set ) ; 
#                    | count ( Register_set ) ; 
#                    | Register_set ;
# 
#   Register_set     : ALL 
#                    | filter Register_set by Predicate_list
#
#   Collection_list  : Collection, Collection_list
#                    | Collection
# 
#   Collection       : { provinces }
#                    | { municipalities }
# 
#   Predicate_list   : Predicate, Predicate_list 
#                    | Predicate 
#
#   Predicate        : time ( date , date )
#                    | location ( string )
#
# -----------------------------------------------------------------------------

# Write functions for each grammar rule which is
# specified in the docstring.
def p_group(p):
    '''
    Program : GROUP Register_set BY Collection_list END
    '''
    p[0] = GroupOp(p[2], p[4])
    
def p_users(p):
    '''
    Program : USER LPAREN Register_set RPAREN END
    '''
    p[0] = Users(p[3])

def p_towers(p):
    '''
    Program : TOWER LPAREN Register_set RPAREN END
    '''
    p[0] = Towers(p[3])
    
def p_count(p):
    '''
    Program : COUNT LPAREN Register_set RPAREN END
    '''
    p[0] = Count(p[3])

def p_program_register_set(p):
    '''
    Program : Register_set END
    '''
    p[0] = p[1]
    
def p_all(p):
    '''
    Register_set : ALL
    '''
    p[0] = AllRegisters()

def p_filter(p):
    '''
    Register_set : FILTER ALL BY Predicate_list
    '''
    # p is a sequence that represents rule contents.
    #
    #  Register_set : filter  ALL     by    Predicate_list
    #      p[0]     :  p[1]   p[2]   p[3]      p[4]
    # 
    p[0] = FilterOp(p[2], p[4])
    
def p_collection_list(p):
    '''
    Collection_list : Collection COMMA Collection_list
                    | Collection
    '''
    if (len(p) == 4):
        p[0] = [p[1]].extend(p[3])
    elif (len(p) == 2):
        p[0] = [p[1]]
        
def p_collection(p):
    '''
    Collection  : LBRACE PROV RBRACE
                | LBRACE MUN RBRACE
    '''
    if(p[2] == 'PROVINCES'):
        p[0]= ProvincesCollection()
    elif p[2] =='MUNICIPALITIES':
        p[0]= MunicipalitiesCollection()

def p_predicate_list(p):
    '''
    Predicate_list : Predicate COMMA Predicate_list
                   | Predicate
    '''
    if (len(p) == 4):
        p[0] = [p[1]].extend(p[3])
    elif (len(p) == 2):
        p[0] = [p[1]]

def p_predicate(p):
    '''
    Predicate  : TIME LPAREN DATE COMMA DATE RPAREN
               | LOCATION LPAREN STRING RPAREN
    '''
    if(p[1] == 'time'):
        p[0]= TimePredicate(p[3], p[5])
    elif p[1] =='location':
        p[0]= LocationPredicate(p[3])

def p_error(p):
    print(f'Syntax error at {p.value!r}')

# Build the parser
parser = yacc()

# Parse an expression
ast = parser.parse('''filter ALL by time ( 1-12-3988 , 7-8-9878 )''')
print(ast)
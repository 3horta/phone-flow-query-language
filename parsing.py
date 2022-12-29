from ply.yacc import yacc
from lexer import *
from abstract_syntax_tree import *

# -----------------------------------------------------------------------------
#   Grammar
#
#   Program          : SuperType id = Expression ;
#                    | id = Expression ;
#
#   SuperType        : list(Type) 
#                    | ClusterSet 
#                    | Type
#
#   ClusterSet       : clusterset(string, ClusterSet) 
#                    | clusterset(string, registerset)
#
#   Type             : registerset 
#                    | int 
#                    | string 
#                    | date
#
#   Expression       : group Register_set by Collection_list
#                    | users ( Register_set )
#                    | towers ( Register_set )
#                    | count ( Register_set )
#                    | Register_set
# 
#   Register_set     : id 
#                    | ALL
#                    | filter Register_set by Predicate_list
#
#   Collection_list  : Collection, Collection_list
#                    | Collection
# 
#   Collection       : { provinces }
#                    | { municipalities }
#                    | id
# 
#   Predicate_list   : Predicate, Predicate_list 
#                    | Predicate 
#
#   Predicate        : time ( date , date )
#                    | location ( string )
#                    | id
# -----------------------------------------------------------------------------

# Write functions for each grammar rule which is
# specified in the docstring.

def p_variable(p):
    '''
    Program : SuperType ID EQUAL Expression END
            | ID EQUAL Expression END
    '''
    if len(p) == 6:
        p[0] = VariableDeclaration(p[1], p[2], p[4])
    elif len(p) == 5:
        p[0] = VariableAssignment(p[1], p[3])

def p_supertype_list(p):
    '''
    SuperType : TYPE LPAREN Type RPAREN
    '''
    p[0] = p[1] + p[2] + p[3] + p[4]
    
def p_supertype_clusterset(p):
    '''
    SuperType : ClusterSet
    '''
    p[0] = p[1]

def p_supertype_type(p):
    '''
    SuperType : Type
    '''
    p[0] = p[1]

def p_clusterset_clusterset(p):
    '''
    ClusterSet : TYPE LPAREN TYPE COMMA ClusterSet RPAREN
    '''
    p[0] = p[1] + p[2] + p[3] + p[4] + p[5] + p[6]
    
def p_clusterset_registerset(p):
    '''
    ClusterSet : TYPE LPAREN TYPE COMMA TYPE RPAREN
    '''
    p[0] = p[1] + p[2] + p[3] + p[4] + p[5] + p[6]
    
def p_type(p):
    '''
    Type : TYPE
    '''
    p[0] = p[1]

def p_group(p):
    '''
    Expression : GROUP Register_set BY Collection_list
    '''
    p[0] = GroupOp(p[2], p[4])
    
def p_users(p):
    '''
    Expression : USER LPAREN Register_set RPAREN
    '''
    p[0] = Users(p[3])

def p_towers(p):
    '''
    Expression : TOWER LPAREN Register_set RPAREN
    '''
    p[0] = Towers(p[3])
    
def p_count(p):
    '''
    Expression : COUNT LPAREN Register_set RPAREN
    '''
    p[0] = Count(p[3])

def p_expression_register_set(p):
    '''
    Expression : Register_set
    '''
    p[0] = p[1]
    
    
def p_registerset_id(p):
    '''
    Register_set : ID
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
    Collection  : ID
                | LBRACE PROV RBRACE
                | LBRACE MUN RBRACE
    '''
    if len(p) == 2:
        p[0] = p[1]
    elif p[2] == 'PROVINCES':
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
               | ID
    '''
    if len(p) == 2:
        p[0] = p[1]
    elif p[1] == 'time':
        p[0]= TimePredicate(p[3], p[5])
    elif p[1] =='location':
        p[0]= LocationPredicate(p[3])

def p_error(p):
    print(f'Syntax error at {p.value!r}')

# Build the parser
parser = yacc()

# Parse an expression
ast = parser.parse('''filter ALL by time ( 1-12-3988 , 7-8-9878 );''')
print(ast)
from ply.yacc import yacc
from lexer import *
from abstract_syntax_tree import *

# -----------------------------------------------------------------------------
#   Grammar
#
#   Program          : Statement; Program
#                    | Statement;
#
#   Statement        : Type id = Expression
#                    | id = Expression
#                    | function ReturnType id (Parameters) { FunctionBody }
#                    | Expression
#
#   ReturnType       : Type
#                    | void
#
#   FunctionBody     : Program
#                    | Program ReturnStatement
#                    | ReturnStatement
#
#   ReturnStatement  : return Expression;
#
#   Parameters       : Type id ExtraParameters
#                    | epsilon
#
#   ExtraParameters  : , Type id ExtraParameters
#                    | epsilon
#
#   Type             : SimpleType
#                    | ComplexType
#
#   SimpleType       : type
#
#   ComplexType      : list(type)
#                                      
#   Expression       : group Subexpression by { Collection_list }
#                    | users ( Subexpression )
#                    | towers ( Subexpression )
#                    | count ( Subexpression )
#                    | Subexpression
#
#   Subexpression    : id 
#                    | ALL
#                    | filter Subexpression by { Predicate_list }
#                    | id ( Arguments )
#
#   Arguments        : Expression ExtraArguments
#                    | epsilon
#
#   ExtraArguments   : , Expression ExtraArguments
#                    | epsilon
#
#   Collection_list  : Collection, Collection_list
#                    | Collection
# 
#   Collection       : provinces
#                    | municipalities
#                    | id
# 
#   Predicate_list   : Predicate, Predicate_list 
#                    | Predicate 
#
#   Predicate        : time ( date, date )
#                    | location ( string )
#                    | id
# -----------------------------------------------------------------------------

# Write functions for each grammar rule which is
# specified in the docstring.

def p_program(p):
    '''
    Program : Statement_list
    '''
    p[0] = Program(p[1])

def p_statement_list(p):
    '''
    Statement_list : Statement END Statement_list
                   | Statement END
    '''
    if (len(p) == 4):
        p[0] = [p[1]] + p[3]
    elif (len(p) == 3):
        p[0] = [p[1]]


def p_variable(p):
    '''
    Statement : Type ID EQUAL Expression
              | ID EQUAL Expression
              | Expression
    '''
    if len(p) == 5:
        p[0] = VariableDeclaration(p[1], p[2], p[4])
    elif len(p) == 4:
        p[0] = VariableAssignment(p[1], p[3])
    elif len(p) == 2:
        p[0] = p[1]
        
def p_function(p):
    '''
    Statement : FUNCTION ReturnType ID LPAREN Parameters RPAREN LBRACE FunctionBody RBRACE
    '''
    if len(p) == 10:
        p[0] = FunctionDeclaration(p[2], p[3], p[5], p[8])
        
def p_return_type(p):
    '''
    ReturnType : Type
               | VOID
    '''
    p[0] = p[1]
    
def p_function_body(p):
    '''
    FunctionBody : Program
                 | Program ReturnStatement
                 | ReturnStatement
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = [p[1]] + [p[2]]
        
def p_return_statement(p):
    '''
    ReturnStatement : RETURN Expression END
    '''
    p[0] = ReturnStatement(p[2])
        
def p_parameters(p):
    '''
    Parameters : Type ID ExtraParameters
               | empty
    '''
    if len(p) == 4:
        p[0] = [(p[1], p[2])] + p[3]
    else:
        p[0] = []
        
def p_extra_parameters(p):
    '''
    ExtraParameters : COMMA Type ID ExtraParameters
                    | empty
    '''
    if len(p) == 5:
        p[0] = [(p[2], p[3])] + p[4]
    else:
        p[0] = []

def p_type(p):
    '''
    Type : SimpleType
         | ComplexType
    '''
    p[0] = p[1]

def p_complextype(p):
    '''
    ComplexType : COMPLEXTYPE LPAREN TYPE RPAREN
    '''
    p[0] = p[1] + p[2] + p[3] + p[4]
    
    
""" def p_supertype_clusterset(p):
    '''
    SuperType : ClusterSet
    '''
    p[0] = p[1] """

""" def p_supertype_type(p):
    '''
    SuperType : Type
    '''
    p[0] = p[1] """

""" def p_clusterset_clusterset(p):
    '''
    ClusterSet : TYPE LPAREN TYPE COMMA ClusterSet RPAREN
    '''
    p[0] = p[1] + p[2] + p[3] + p[4] + p[5] + p[6] """
    
""" def p_clusterset_registerset(p):
    '''
    ClusterSet : TYPE LPAREN TYPE COMMA TYPE RPAREN
    '''
    p[0] = p[1] + p[2] + p[3] + p[4] + p[5] + p[6] """
    
def p_simpletype(p):
    '''
    SimpleType : TYPE
    '''
    p[0] = p[1]

def p_group(p):
    '''
    Expression : GROUP Subexpression BY LBRACE Collection_list RBRACE
    '''
    p[0] = GroupOp(p[2], p[5])
    
def p_users(p):
    '''
    Expression : USER LPAREN Subexpression RPAREN
    '''
    p[0] = Users(p[3])

def p_towers(p):
    '''
    Expression : TOWER LPAREN Subexpression RPAREN
    '''
    p[0] = Towers(p[3])
    
def p_count(p):
    '''
    Expression : COUNT LPAREN Subexpression RPAREN
    '''
    p[0] = Count(p[3])

def p_expression_subexpression(p):
    '''
    Expression : Subexpression
    '''
    p[0] = p[1]
    
    
def p_subexpression_id(p):
    '''
    Subexpression : ID
    '''
    p[0] = VariableCall(p[1])
    
def p_all(p):
    '''
    Subexpression : ALL
    '''
    p[0] = AllRegisters()

def p_filter(p):
    '''
    Subexpression : FILTER Subexpression BY LBRACE Predicate_list RBRACE
    '''
    # p is a sequence that represents rule contents.
    #
    #  Subexpression : filter  Subexpression     by    Predicate_list
    #      p[0]     :  p[1]       p[2]        p[3]      p[4]
    
    p[0] = FilterOp(p[2], p[5])
    
def p_function_call(p):
    '''
    Subexpression : ID LPAREN Arguments RPAREN
    '''
    p[0] = FunctionCall(p[1], p[3])
    
def p_arguments(p):
    '''
    Arguments : Expression ExtraArguments
              | empty
    '''
    if len(p) == 3:
        p[0] = [p[1]] + p[2]
    else:
        p[0] = []
        
def p_extra_arguments(p):
    '''
    ExtraArguments : COMMA Expression ExtraArguments
                   | empty
    '''
    if len(p) == 4:
        p[0] = [p[2]] + p[3]
    else:
        p[0] = []

def p_empty(p):
    '''
    empty :
    '''
    pass
    
def p_collection_list(p):
    '''
    Collection_list : Collection COMMA Collection_list
                    | Collection
    '''
    if (len(p) == 4):
        p[0] = [p[1]] + p[3]
    else:
        p[0] = [p[1]]
        
def p_collection(p):
    '''
    Collection  : ID
                | PROV
                | MUN
    '''
    if p[1] == 'PROVINCES':
        p[0] = ProvincesCollection()
    elif p[1] =='MUNICIPALITIES':
        p[0] = MunicipalitiesCollection()
    else:
        p[0] = VariableCall(p[1])

def p_predicate_list(p):
    '''
    Predicate_list : Predicate COMMA Predicate_list
                   | Predicate
    '''
    if (len(p) == 4):
        p[0] = [p[1]] + p[3]
    else:
        p[0] = [p[1]]

def p_predicate(p):
    '''
    Predicate  : TIME LPAREN DATE COMMA DATE RPAREN
               | LOCATION LPAREN STRING RPAREN
               | ID
    '''
    if len(p) == 2:
        p[0] = VariableCall(p[1])
    elif p[1] == 'time':
        p[0]= TimePredicate(p[3], p[5])
    elif p[1] =='location':
        p[0]= LocationPredicate(p[3])

def p_error(p):
    raise Exception(f"Syntax error at '{p.value}', line {p.lineno} (Index {p.lexpos}).")

# Build the parser
parser = yacc()


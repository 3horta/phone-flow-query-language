from ply.yacc import yacc

from abstract_syntax_tree import *
from lexer import *

# -----------------------------------------------------------------------------
#   Grammar
#
#   Program          : StatementList
#
#   StatementList    : Statement; StatementList
#                    | Statement;
#
#   Statement        : Type id = Assignable
#                    | id = Assignable
#                    | function ReturnType id (Parameters) { Body }
#                    | Expression
#                    | if ( Condition ) { Body }
#                    | while ( Condition ) { Body }
#                    | show ( Assignable )
#  
#   Assignable       : Expression
#                    | Literal
#                    | ArithmeticOp
#
#   ArithmeticOp     : ArithmeticOp + NumLit
#                    | ArithmeticOp - NumLit
#                    | NumLit
#                    | ArithmeticOp + id
#                    | ArithmeticOp - id
#                    | id + id
#                    | id - id
#                    | id + NumLit
#                    | id - NumLit
#
#   NumLit           : ( ArithmeticOp )
#                    | num
#
#   Literal          : bool
#                    | CollectionLit
#
#
#   Condition        : Assignable comparer Assignable
#                    | bool
#
#   ReturnType       : Type
#                    | void
#
#   Body             : StatementList
#                    | StatementList ReturnStatement
#                    | ReturnStatement
#
#   ReturnStatement  : return Assignable;
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
#                    | booltype
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
#   Arguments        : Assignable ExtraArguments
#                    | epsilon
#
#   ExtraArguments   : , Assignable ExtraArguments
#                    | epsilon
#
#   Collection_list  : Collection, Collection_list
#                    | Collection
# 
#   Collection       : CollectionLit
#                    | id
#
#   CollectionLit    : provinces
#                    | municipalities
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
    Program : StatementList
    '''
    p[0] = Program(p[1])

def p_statement_list(p):
    '''
    StatementList : Statement END StatementList
                  | Statement END
    '''
    if (len(p) == 4):
        p[0] = [p[1]] + p[3]
    elif (len(p) == 3):
        p[0] = [p[1]]


def p_variable(p):
    '''
    Statement : Type ID EQUAL Assignable
              | ID EQUAL Assignable
              | Expression
              | SHOW LPAREN Assignable RPAREN
    '''
    if len(p) == 5 and p[1] == 'show':
        p[0] = Show(p[3])
    elif len(p) == 5:
        p[0] = VariableDeclaration(p[1], p[2], p[4])
    elif len(p) == 4:
        p[0] = VariableAssignment(p[1], p[3])
    elif len(p) == 2:
        p[0] = p[1]
        
def p_function(p):
    '''
    Statement : FUNCTION ReturnType ID LPAREN Parameters RPAREN LBRACE Body RBRACE
    '''
    if len(p) == 10:
        p[0] = FunctionDeclaration(p[2], p[3], p[5], p[8])

def p_if(p):
    '''
    Statement : IF LPAREN Condition RPAREN LBRACE Body RBRACE
              | WHILE LPAREN Condition RPAREN LBRACE Body RBRACE
    '''
    if p[1] == 'if':
        p[0] = IfStatement(p[3], p[6])
    else:
        p[0] = WhileStatement(p[3], p[6])
        

def p_asignable(p):
    '''
    Assignable : Expression
               | Literal
               | ArithmeticOp
    '''
    p[0] = p[1]
    
def p_arithmetic_op(p):
    '''
    ArithmeticOp : ArithmeticOp PLUS NumLit
                 | ArithmeticOp MINUS NumLit
                 | NumLit
    '''
    if len(p) == 4:
        p[0] = ArithmeticOp(p[1], p[2], p[3])
    else:
        p[0] = p[1]
        
def p_arithmetic_op_id(p):
    '''
    ArithmeticOp : ArithmeticOp PLUS ID
                 | ArithmeticOp MINUS ID
    '''
    p[0] = ArithmeticOp(p[1], p[2], VariableCall(p[3]))
    
def p_arithmetic_op_id_id(p):
    '''
    ArithmeticOp : ID PLUS ID
                 | ID MINUS ID
    '''
    p[0] = ArithmeticOp(VariableCall(p[1]), p[2], VariableCall(p[3]))
    
def p_arithmetic_op_id_numlit(p):
    '''
    ArithmeticOp : ID PLUS NumLit
                 | ID MINUS NumLit
    '''
    p[0] = ArithmeticOp(VariableCall(p[1]), p[2], p[3])

def p_numlit(p):
    '''
    NumLit : LPAREN ArithmeticOp RPAREN
           | NUM
    '''
    if len(p) == 4:
        p[0] = p[2]
    elif p.slice[1].type == 'NUM':
        p[0] = Literal(p[1], p.slice[1].type)

def p_literal(p):
    '''
    Literal : BOOL
    '''
    p[0]= Literal(p[1], p.slice[1].type)

def p_literal_collection(p):
    '''
    Literal : CollectionLit
    '''
    p[0] = p[1]

def p_condition(p):
    '''
    Condition : Assignable GEQUAL Assignable
              | Assignable LEQUAL Assignable
              | Assignable EQUALEQUAL Assignable
              | Assignable GREATER Assignable
              | Assignable LESS Assignable
              | BOOL
    '''
    if len(p) == 2:
        p[0]= Literal(p[1], p.slice[1].type)
    else:
        p[0] = BinaryComparer(p[1], p[2], p[3])
        
def p_return_type(p):
    '''
    ReturnType : Type
               | VOID
    '''
    p[0] = p[1]
    
def p_body(p):
    '''
    Body : ReturnStatement
         | StatementList ReturnStatement
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[2]]

def p_body_no_ret(p):
    '''
    Body : StatementList
    '''
    p[0] = p[1]
        
def p_return_statement(p):
    '''
    ReturnStatement : RETURN Assignable END
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
    
def p_simpletype(p):
    '''
    SimpleType : TYPE
               | BOOLTYPE
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
    Arguments : Assignable ExtraArguments
              | empty
    '''
    if len(p) == 3:
        p[0] = [p[1]] + p[2]
    else:
        p[0] = []
        
def p_extra_arguments(p):
    '''
    ExtraArguments : COMMA Assignable ExtraArguments
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
    '''
    p[0] = VariableCall(p[1])

def p_collectio(p):
    '''
    Collection  : CollectionLit
    '''
    p[0]= p[1]

def p_collection_literal(p):
    '''
    CollectionLit : PROV
                  | MUN
    '''
    if p[1] == 'PROVINCES':
        p[0] = ProvincesCollection()
    elif p[1] =='MUNICIPALITIES':
        p[0] = MunicipalitiesCollection()

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


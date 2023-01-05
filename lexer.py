import ply.lex as lex

# Keywords
reserved = {
   'time' : 'TIME',
   'location' : 'LOCATION',
   'ALL' : 'ALL',
   'filter' : 'FILTER',
   'by': 'BY',
   'group' : 'GROUP',
   'users' : 'USER',
   'towers' : 'TOWER',
   'count' : 'COUNT',
   'PROVINCES' : 'PROV',
   'MUNICIPALITIES' : 'MUN',
   'registerset' : 'TYPE',
   'int' : 'TYPE',
   'string' : 'TYPE',
   'date' : 'TYPE',
   'list' : 'COMPLEXTYPE',
   'clusterset' : 'TYPE',
   'function' : 'FUNCTION',
   'void' : 'VOID',
   'return' : 'RETURN',
   'bool' : 'BOOLTYPE',
   'true' : 'BOOL',
   'false' : 'BOOL',
   'if' : 'IF',
   'show' : 'SHOW',
   'while' : 'WHILE'
}

# List of token names. 
tokens = (
   'STRING',
   'DATE',

   'PLUS',
   'MINUS',
   'MULTIPLY',
   'DIFFER',

   'ID',
   'NUM',
   
   'EQUALEQUAL',
   'GEQUAL',
   'LEQUAL',
   'LESS',
   'GREATER',

   'LPAREN',
   'RPAREN',
   'EQUAL',
   'COMMA',
   'END',
   'LBRACE',
   'RBRACE'
   
)


# Regular expression rules for simple tokens
t_STRING = r'"\w*"'
#t_DATE = r'((\d\d?-)?\d\d?-)?\d{4}'

t_PLUS = r'\+'
t_MINUS = r'\-'
t_MULTIPLY = r'\*'
t_DIFFER = r'\\'

t_EQUALEQUAL = r'=='
t_GEQUAL = r'>='
t_LEQUAL = r'<='
t_LESS = r'<'
t_GREATER = r'>'

t_LPAREN  = r'\('
t_RPAREN  = r'\)'
t_EQUAL= r'='
t_COMMA= r','
t_END= r';'
t_LBRACE = r'\{'
t_RBRACE = r'\}'

def t_DATE(t):
    r'((\d\d?-)?\d\d?-)?\d{4}'
    return t


def t_NUM(t):
    r'\d+'
    t.value = int(t.value)
    return t

def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9]*'
    t.type = reserved.get(t.value, 'ID')    # Check for reserved words
    return t

def t_COMMENT(t):
    r'\#.*'
    pass
    # No return value. Token discarded

# Define a rule so we can track line numbers
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)


# A string containing ignored characters (spaces and tabs)
t_ignore  = ' \t'

# Error handling rule
def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    raise Exception(f"Invalid token '{t.value[0]}' at line {t.lineno} (Index {t.lexpos}).")

def find_column(input, token):
    '''
        Compute column.
        Input is the input text string.
        Token is a token instance.
    '''
    
    line_start = input.rfind('\n', 0, token.lexpos) + 1
    return (token.lexpos - line_start) + 1

tokens= list(reserved.values()) + list(tokens)

lexer = lex.lex(debug=True)

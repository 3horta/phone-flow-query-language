import ply.lex as lex

reserved = {
   'time' : 'TIME',
   'location' : 'LOCATION',
   'ALL' : 'ALL',
   'filter' : 'FILTER',
   'by': 'BY'
}

# List of token names. 
tokens = (
   'STRING',
   'DATE',

   #'TIME',
   #'LOCATION'
   #'ALL'

   #'FILTER',
   #'BY',
   'PLUS',
   'MULTIPLY',
   'DIFFER',

   'ID',

   'LPAREN',
   'RPAREN',
   'EQUAL',
   'COMMA'
   
)



# Regular expression rules for simple tokens


t_STRING= r'"\w*"'

t_DATE= r'\d\d?-\d\d?-\d\d\d\d'  #DUDA: Desde aqui lo convierto en Date de Python? Como hacen con numeros en ejemplo

#t_TIME= r'time'
#t_LOCATION= r'location'
#t_ALL = r'ALL'

#t_FILTER= r'filter'
#t_BY= r'by'

t_PLUS    = r'\+'
t_MULTIPLY= r'\*'
t_DIFFER  = r'\\'



t_LPAREN  = r'\('
t_RPAREN  = r'\)'
t_EQUAL= r'='
t_COMMA= r','

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
    t.lexer.skip(1) 

# Compute column.
#     input is the input text string
#     token is a token instance
def find_column(input, token):
    line_start = input.rfind('\n', 0, token.lexpos) + 1
    return (token.lexpos - line_start) + 1

tokens= list(reserved.values()) + list(tokens)


lexer = lex.lex()

data = '''

'''

lexer.input(data)

for tok in lexer:
    print(tok)

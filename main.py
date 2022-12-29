from parsing import parser
from lang.context import Context
# Parse an expression
ast = parser.parse('''int a = filter ALL by time ( 1-12-3988 , 7-8-9878 );''')

ast.evaluate(Context())
print(ast)
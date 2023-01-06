from lang.context import Context
from lang.semantic_checker import SemanticChecker
from parsing import parser

print("Insert file name ( with extension '.pfql' ) to run: ")
file_name = input()

with open(file_name + '.pfql', 'r') as file:
    data = file.read()

ast = parser.parse(data)

type_checker = SemanticChecker(Context())
type_checker.visit(ast)

ast.evaluate(Context())
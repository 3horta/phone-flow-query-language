# PFQL: phone-flow-query-language
## Lenguaje de dominio específico para el trabajo con datos de telefonía móvil

phone-flow-query-language(PFQL) es un lenguaje de dominio específico que se propone como proyecto final de la asignatura Compilación. Tiene como objetivo facilitar el trabajo con datos de telefonía movil que se desarrolla en el Centro de Sistemas Complejos, ubicado en la Facultad de Física de la Universidad de La Habana. Actualmete el trabajo con estos datos se realiza utilizando Spark que brinda numerosas facilidades por su expresividad y eficiencia. Sin embargo, se busca crear un DSL para poder realizar operaciones específicas del dominio de forma más simple y ampliando la posibilidad de que personas sin muchos conocimientos sobre programación y trabajo con datos puedan acceder a la información que necesitan. 

## Sintaxis del Lenguaje
PFQL es un lenguaje no orientado a objetos, con una sintaxis con características funcionales y con tipado estático.

### Palabras reservadas:
- time
- location
- ALL
- filter
- by
- group
- users
- towers
- count
- PROVINCES
- MUNICIPALITIES
- registerset
- int
- string
- date
- list
- clusterset
- function
- void
- return
- bool
- true
- false
- if
- show
- while

### Declaración y uso de variables:

``` python
int i = 1;
i = 2;
int j = i;
```

### Condicionales:
``` python
if (users(ALL) == d) {
     return d;
  };
```

### Ciclos:
``` python
int a = 0;
while (a < 10) {
  show(a);
  a = a + 1;
};
```

### Funciones built in:


``` python
registerset a = filter ALL by { time ( 1-12-3988 , 7-8-9878 ), time(1200, 1209) }; 
```
``` python
clusterset b = group a by { MUNICIPALITIES, PROVINCES };
```
``` python
users(ALL);
```
``` python
towers(a);
```
``` python
count(a);
```
``` python
show(a);
```

### Definición y llamado a funciones:

Definición:
``` 
function <tipo_de_retorno> <nombre_de_la_función> (<parámetros>)
{
   <Cuerpo de la función>
};
```
donde los parámetos son de la forma ``< tipo id, ..., tipo id>``

Por ejemplo:

```python
function int Fibonacci(int n) {
    if (n == 1) {
        return 0;
    };
    if (n == 2) {
        return 1;
    };
    return Fibonacci(n - 1) + Fibonacci(n - 2);
};

show(Fibonacci(6));  # Llamado a función 
```

## Características de la Grámatica
La gramática del lenguaje es una gramatica LALR. 

### Reglas de la gramática:
```
   Program          : StatementList

   StatementList    : Statement; StatementList
                    | Statement;

   Statement        : Type id = Assignable
                    | id = Assignable
                    | function ReturnType id (Parameters) { Body }
                    | Expression
                    | if ( Condition ) { Body }
                    | while ( Condition ) { Body }
                    | show ( Assignable )
  
   Assignable       : ArithmeticOp
                    | Literal

   ArithmeticOp     : ArithmeticOp + NumLit
                    | ArithmeticOp - NumLit
                    | NumLit
                    | ArithmeticOp + Expression
                    | ArithmeticOp - Expression
                    | Expression

   NumLit           : ( ArithmeticOp )
                    | num

   Literal          : bool
                    | CollectionLit

   Condition        : Assignable comparer Assignable
                    | bool

   ReturnType       : Type
                    | void

   Body             : StatementList
                    | StatementList ReturnStatement
                    | ReturnStatement

   ReturnStatement  : return Assignable;

   Parameters       : Type id ExtraParameters
                    | epsilon

   ExtraParameters  : , Type id ExtraParameters
                    | epsilon

   Type             : SimpleType
                    | ComplexType

   SimpleType       : type
                    | booltype

   ComplexType      : list(type)
                                      
   Expression       : group Subexpression by { Collection_list }
                    | users ( Subexpression )
                    | towers ( Subexpression )
                    | count ( Subexpression )
                    | Subexpression

   Subexpression    : id
                    | ALL
                    | filter Subexpression by { Predicate_list }
                    | id ( Arguments )

   Arguments        : Assignable ExtraArguments
                    | epsilon

   ExtraArguments   : , Assignable ExtraArguments
                    | epsilon

   Collection_list  : Collection, Collection_list
                    | Collection
 
   Collection       : CollectionLit
                    | id

   CollectionLit    : provinces
                    | municipalities

   Predicate_list   : Predicate, Predicate_list 
                    | Predicate 

   Predicate        : time ( date, date )
                    | location ( string )
                    | id
```
## Arquitectura del Compilador 
### Lexer:

Para el análisis léxico se utilizó el módulo ``lex`` de la biblioteca ``ply`` de Python. 
Se definieron las palabras reservadas del lenguaje y las expresiones regulares para reconocer los tokens del lenguaje. Para el trabajo con expresiones regulares se utiliza la biblioteca ``re``. 
Los token son instancias de ``LexToken`` y tienen los atributos ``type``, ``value``, ``lineno`` y ``lexpos``. Computar estos dos últimos atributos resulta útil para indicar al programador en caso de haber error léxico en que línea y posición del código se encentra el token inválido. 
Se utilizó el caracter ``#`` para indicar el comentario y el caracter ``;`` para indicar el fin de una instrucción.

### Parser:
Para el proceso de parsing se utilizó el módulo ``yacc`` de la biblioteca ``ply`` de Python y se definieron las reglas semánticas para indicar el comportamiento semántico del lenguaje y la construcción del árbol de sintaxis abtracta (AST). Yacc usa un parser LALR. Cada regla de la grámatica se especifica como una función de ``Python`` donde el docstring de la función indica la regla gramática que corresponde. En el archivo ``parsing.out`` se muestra como queda la gramática y el autómata LALR correspondiente. 

A continuación se muestra un ejemplo de la definición de una regla de la gramática y la regla semántica correspondiente:

```python 
def p_program(p):
 ''' Program : Statement_list'''
  p[0] = Program(p[1]) 
 ```

### AST:

Para la construcción del AST se creó una clase abstracta ``Node`` de la cual heredan el resto de los nodos del AST. 

Cada nodo tiene un atributo ``computed_type`` que se computa y utiliza en la fase de verificación semántica. 

Cada nodo tiene un método ``evaluate`` que se utiliza para computar el valor del nodo. El método ``evaluate`` recibe una instancia de la clase ``Context`` de la que obtiene la información del programa de pfql que necesite y añade la información que corresponda. 

### Verificación semántica:

Para la fase de verificación semántica se creó la clase ``SemanticChecker`` que tiene una instancia del contexto. Se empleó el patrón visitor para visistar los nodos del AST y realizar el chequeo de tipos correspondiente, así como obtener el tipo de cada nodo. Todos los errores de tipo del programa son detectados en esta fase, previa a la ejecución. 

A continuación algunas reglas semánticas del lenguaje:
- En la declaración de una función el tipo debe coincidir con el tipo de la expresión asociada. 
- Dos variables no pueden tener el mismo nombre.
- La asignación o llamado a una variable solo puede hacerse sobre variables previamemte 
– Dos funciones dentro de un mismo scope no pueden tener el mismo nombre.
- El llamado a una función o variable debe hacerse luego de su declaración
- En el llamado a una función deben pasarse todos los argumentos de esta en el mismo orden en que se definieron los parámetros en la declaración.
- Pueden definirse funciones dentro de otras y estas solo serán accesibles en ese contexto. 
- Las funciones no pueden sobrescribirse
- El tipo de retorno de una función tiene que coincidir con los tipos de aquellas expresiones que se encuentren dentro de un ``return statement``. Cuando el tipo de retorno es ``void`` no puede existir ningún ``return statement`` en el cuerpo de la función.
- La condición de uns instrucción ``if`` o ``while`` debe ser de tipo ``bool``
- Las operaciones de comparación ``>``, ``<``, ``>=``, ``<=`` están definidas para operandos del tipo ``int``
- La operación ``==`` está definida para operandos del mismo tipo. 
- Las operaciones ``+``,`` −``  están definidas para los tipos ``string`` , ``list(string)``, ``int``, ``registerset`` y  ``clusterset`` y debe ser entre expresiones del mismo tipo. 
- La operacion ``group`` está definida para instancias de ``registerset`` y los predicados por los que se permite filtar son de tipo ``string`` y ``time_interval``. En el caso del ``string`` debe ser una provincia o municipio válido. 
- Las funciones ``users``, ``towers`` y ``count`` están definidas para el tipo ``registerset``

### Ejecución de un programa:

Para la ejecución se utiliza el método ``evaluate`` de los nodos del ``AST``. A partir del nodo ``Program`` se evalúan recursivamente cada uno de los nodos del ``AST`` construido. Las instrucciones propias del dominio se evalúan llamando al método correspondiente de la ``API`` definida sobre PySpark.

#### API:
En la API se definen los métodos para el trabajo con los datos. Estos son archivos ``.parquet`` que se cargan en forma de DataFrame de la biblioteca ``Pandas``. El resto de las funciones se encargan de la manipulación de los DataFrames y son utilizadas en el ``AST`` para evaluar los nodos.


## Comando para correr la aplicacion 
Ejecutar ``main.py`` y como se indica en la consola introducir el nombre del archivo de expresión ``.pfql`` de la carpeta ``tester`` que se desea ejecurar.
Para ejecutar un archivo creado por usted debe ponerle la extensión ``.pfql`` y ponerlo en la carpeta ``tester``. Luego seguir las instrucciones anteriores. 

## Ejemplos de código en PFQL
En la carpeta ``tester`` ubicada en la raíz hay varios archivos con ejemplos. 

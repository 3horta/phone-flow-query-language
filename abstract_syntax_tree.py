
class Expression: 
    pass

class FilterOp(Expression):
    def __init__(self, op, registers, predicates) -> None:
        self.op = op
        self.registers = registers
        self.predicates = predicates

class Predicate(Expression):
    def __init__(self, predicate) -> None:
        self.predicate = predicate

class TimePredicate(Predicate):
    def __init__(self, predicate, start_date, end_date) -> None:
        super().__init__(predicate)
        self.start_date = start_date
        self.end_date = end_date
        
class LocationPredicate(Predicate):
    def __init__(self, predicate, location) -> None:
        super().__init__(predicate)
        self.location = location
        
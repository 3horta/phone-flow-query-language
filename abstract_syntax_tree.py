
class Expression: 
    def evaluate(self):
        pass

class FilterOp(Expression):
    def __init__(self, op, registers, predicates) -> None:
        self.op = op
        self.registers = registers
        self.predicates = predicates
    def evaluate(self):
        pass

class Predicate(Expression):
    def __init__(self, predicate) -> None:
        self.predicate = predicate
    def evaluate(self):
        pass

class TimePredicate(Predicate):
    def __init__(self, predicate, start_date, end_date) -> None:
        super().__init__(predicate)
        self.start_date = start_date
        self.end_date = end_date
    def evaluate(self):
        pass
        
class LocationPredicate(Predicate):
    def __init__(self, predicate, location) -> None:
        super().__init__(predicate)
        self.location = location
    def evaluate(self):
        pass
        
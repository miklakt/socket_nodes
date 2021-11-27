import logging
logger = logging.getLogger(__name__)
class BaseExecutorClass:
    """
    Class to be inherited to construct user ExecutorNode
    """
    def __init__(self) -> None:
        pass
    def verify(self, expr):
        return True
    def execute(self, expr):
        if self.verify(expr):
            pass
        else:
            raise(ValueError('Request has not passed verification'))

class ExecutorLocalScopeOnly(BaseExecutorClass):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, expr):
        super().execute(expr)
        if isinstance(expr, list):
            return [self.execute_item(expr_line) for expr_line in expr]
        elif isinstance(expr, dict):
            return {request_name:self.execute_item(expr_line) for request_name, expr_line in expr.items()}
        else:
            return self.execute_item(expr)

    def execute_item(self, expr : str):
        try:
            scope = {k: getattr(self, k) for k in dir(self) if '__' not in k}
            return eval(expr,scope)
        except Exception as e:
            return e

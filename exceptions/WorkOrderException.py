class WorkOrderException(Exception):
    """Exception raised when an error occurs in the WorkOrder class"""

    def __init__(self, message, state, work_order):
        super().__init__(message)
        self.state = state
        self.work_order = work_order

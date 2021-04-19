class PickTicketAlreadyCreatedException(Exception):
    def __init__(self, message):
        super().__init__(message)


class PickTicketNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)


class ContainerTypeDestinationMismatchException(Exception):
    def __init__(self, message):
        super().__init__(message)


class ContainerNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidPackException(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidPickTicketStateException(Exception):
    def __init__(self, message):
        super().__init__(message)


class PoisonMessageException(Exception):
    def __init__(self, message):
        super().__init__(message)

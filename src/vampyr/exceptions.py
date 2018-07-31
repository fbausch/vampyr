#!/usr/bin/python3


class VampyrException(Exception):
    """
    Generic Exception
    """
    pass


class VampyrMagicException(VampyrException):
    """
    Thrown when a data structure header is not as expected.
    """
    pass

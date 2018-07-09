#!/usr/bin/python3


class CephException(Exception):
    pass


class CephEmptyBlockException(CephException):
    pass


class CephUnexpectedMagicException(CephException):
    pass

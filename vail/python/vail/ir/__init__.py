"""Placeholder import namespace for Hail IR tests.

Vail does not currently implement Hail's Python IR. The module exists so
adapted test modules can import it and report unsupported IR use when a test
body reaches it.
"""


def __getattr__(name):
    raise NotImplementedError(f'vail.ir.{name} is not implemented')

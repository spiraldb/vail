"""Minimal context import shim for adapted Hail tests."""


class TemporaryFilename:
    def __init__(self, *args, **kwargs):
        raise NotImplementedError('vail.context.TemporaryFilename is not implemented')

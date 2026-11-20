"""Importable test-only projectors for the installed extension contract."""

import os


def summary(raw):
    return b'{"summary":"ready"}'


def echo_source(raw):
    return raw


def too_large(raw):
    return bytes(257)


def invalid_type(raw):
    return "invalid"


def refused(raw):
    raise ValueError("private-details")


def worker_pid(raw):
    return str(os.getpid()).encode()

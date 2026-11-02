"""A full GC pass must not land inside a test: tests carry wall-clock deadlines."""

import gc


def test_what_collection_built_is_out_of_every_full_gc_pass():
    assert gc.get_freeze_count() > 100_000


def test_no_full_gc_pass_starts_on_its_own_during_a_test():
    assert gc.get_threshold()[2] >= 1_000_000

"""Real inventory HTTP/SQL seams under the whole-protocol blocking-I/O gate."""

import pytest
import test_output_inventory_api as inventory_cases
import test_output_inventory_execution_client as client_cases
from _run_output_capture_test_support import capture_environment as capture_environment  # noqa: F401
from _run_output_capture_test_support import output_store as output_store  # noqa: F401
from test_run_output_route import route_environment as route_environment  # noqa: F401

pytestmark = pytest.mark.asyncio


async def test_actual_sql_inventory_route_and_v1_view(route_environment):
    await inventory_cases.test_real_sql_terminal_pair_and_sources_return_nested_complete_empty(route_environment)


async def test_actual_sql_inventory_read_cancellation(capture_environment, monkeypatch):
    await inventory_cases.test_source_uses_bounded_explicit_scope_and_drains_repeated_cancel(capture_environment, monkeypatch)


async def test_actual_owned_http_inventory_read():
    await client_cases.test_real_owner_negotiates_exact_read_under_retained_source_authority()

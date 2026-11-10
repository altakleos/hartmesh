"""Discovery uses owned HTTP/JSON operations under the strict loop policy."""

from test_telegram_agent_lists import (
    test_cancellation_during_list_decode_drains_before_client_close as test_cancellation_during_list_decode_drains_before_client_close,
)
from test_telegram_agent_lists import (
    test_final_cleanup_cannot_return_a_read_after_its_guard_retires as test_final_cleanup_cannot_return_a_read_after_its_guard_retires,
)
from test_telegram_agent_lists import (
    test_list_read_uses_gateway_owner_maximum_page_and_retains_response as test_list_read_uses_gateway_owner_maximum_page_and_retains_response,
)
from test_telegram_agent_lists import (
    test_lost_read_guard_refuses_and_closes_without_retaining_private_result as test_lost_read_guard_refuses_and_closes_without_retaining_private_result,
)

# Import application modules during collection, as Gateway startup does.
from app.telegram_bots.gateway_client import GatewayClientOwner as GatewayClientOwner

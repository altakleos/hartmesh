"""An image in a managed conversation reaches the model through the real admission.

Directory-backed fixtures stand in for the mounted Space, as elsewhere. The path scope,
the tool and the middleware are the ones a chat turn runs.
"""

from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from test_compose_account_seats import seated_accounts as seated_accounts
from test_compose_user_assignment import account_storage as account_storage
from test_view_image_middleware import _image_urls, _model_request, _view_image_call

from app.gateway import chat_storage
from app.gateway.auth.models import User
from app.gateway.storage_spaces import assign_user_space
from deerflow.agents.middlewares.view_image_middleware import ViewImageMiddleware
from deerflow.config.paths import get_paths
from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.facade import storage_credential_scope
from deerflow.tools.builtins.view_image_tool import _view_image_authorized

JPEG = b"\xff\xd8\xff\xe0" + b"stone" * 8
UPLOAD = "/mnt/user-data/uploads/stone.jpg"


@pytest.mark.asyncio
async def test_an_upload_reaches_the_model_before_the_conversation_has_a_sandbox(account_storage, monkeypatch):
    from deerflow.authz import sandbox_authz

    app, repo, _sf, _files, _catalog, _company = account_storage
    user = await repo.create_user(User(email="vision@example.com"))
    monkeypatch.setattr(sandbox_authz, "authorize_sandbox_execution", lambda **kwargs: None)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_sandbox_provider", lambda: SimpleNamespace(get=lambda sandbox_id: None))
    context = {"user_id": str(user.id), "thread_id": "chat-a"}
    prepared_requests = []

    async def model(request):
        prepared_requests.append(request)
        return AIMessage(content="An amethyst geode.")

    token = set_current_user(user)
    owner = CaptureIOOwner()
    try:
        with capture_io_scope(owner), storage_credential_scope(True):
            async with chat_storage.chat_storage_lifecycle(app, enabled=True):
                await assign_user_space(app, user)
                async with app.state.chat_storage_scope(user_id=context["user_id"], thread_id="chat-a"):
                    paths = get_paths()
                    paths.ensure_thread_dirs("chat-a", user_id=context["user_id"])
                    thread_data = paths.thread_data("chat-a", user_id=context["user_id"])
                    assert "/conversations/chat-a/user-data/uploads" in thread_data["uploads_path"]
                    with open(thread_data["uploads_path"] + "/stone.jpg", "wb") as stream:
                        stream.write(JPEG)
                    result = _view_image_authorized(SimpleNamespace(state={"thread_data": thread_data}, context=context), UPLOAD, "call-image")
                    assert result.update["messages"][0].content == "Successfully read image"
                    messages = [
                        AIMessage(content="", tool_calls=[_view_image_call("call-image", UPLOAD)]),
                        ToolMessage(content="Successfully read image", tool_call_id="call-image"),
                    ]
                    request = _model_request(messages, result.update["viewed_images"])
                    request.state["thread_data"] = thread_data
                    request.runtime.context = context
                    await ViewImageMiddleware().awrap_model_call(request, model)
    finally:
        owner.retire()
        await owner.drain()
        reset_current_user(token)

    assert len(_image_urls(prepared_requests[0])) == 1
    assert _image_urls(prepared_requests[0])[0].startswith("data:image/jpeg;base64,")

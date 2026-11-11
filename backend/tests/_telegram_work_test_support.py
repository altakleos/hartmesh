"""Real account, instance and Work services without a filesystem effect fixture."""

from test_telegram_instance import instance_environment

from deerflow.agent_instances.contract import DefinitionSnapshot
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import CreateRequest
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputReadRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.base import Base
from deerflow.spaces.contract import PrincipalRef


async def work_environment(db, monkeypatch):
    from uuid import uuid4

    e = await instance_environment(db, monkeypatch)
    from deerflow.config import app_config

    monkeypatch.setattr(app_config, "peek_loaded_app_config", lambda: e.live.config)
    models = (AgentLifecycleRow, AgentWorkRow, AgentWorkAttemptRow, AgentWorkEventRow, HumanInputRequestRow, HumanInputResponseRow, HumanInputEventRow, HumanInputReadRow)
    async with db.engine.begin() as connection:
        await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=[model.__table__ for model in models]))
    snapshot = DefinitionSnapshot.capture(owner_id=e.live.id, config={"name": "analyst", "work_policy": {"enabled": True}}, soul="Review documentation.")
    async with db.sessions.begin() as writer:
        writer.add(AgentDefinitionRevisionRow(revision=snapshot.revision, owner_id=snapshot.owner_id, config=snapshot.config, soul=snapshot.soul))
        await writer.flush()
        (await writer.get(AgentInstanceRow, e.link.agent_ref)).definition_revision = snapshot.revision
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.live.id))).permissions = 7
    e.actor = PrincipalRef("human", e.live.id)
    e.instance = await e.http_app.state.agent_instances.get(actor=e.actor, instance_id=e.link.agent_ref)
    e.human = HumanInput(AgentWork(e.http_app.state.agent_instances))
    e.work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Explain verified gaps"))
    e.request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=e.work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex, expected_work_revision=1, expected_assignment_revision=1, purpose="information", question="Which guide?", reason="The source is missing", expected_response="A reference", recipient_id=e.actor.subject_id
        ),
    )
    e.db = db
    return e

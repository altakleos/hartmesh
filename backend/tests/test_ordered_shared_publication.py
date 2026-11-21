"""Native Shared publishes each attempted name only after saving its complete recovery fact."""

import hashlib
import json
import os
from tempfile import TemporaryFile
from uuid import uuid4

import pytest
from test_compose_native_file_sources import native_request
from test_storage_feature_routes import feature_app as feature_app

from app.gateway.chat_files import QualifiedUploadStage
from app.gateway.native_file_sources import NativeFileSources
from app.gateway.native_shared_publications import NativeSharedCopy
from deerflow.features.publications import FACT_BEFORE_NAME, copy_and_record
from deerflow.files.store import DescriptorSource
from deerflow.spaces.contract import Permission, PrincipalRef
from deerflow.spaces.facade import storage_actor_scope
from deerflow.spaces.workflows import WorkflowReceipt


@pytest.mark.asyncio
async def test_receipt_precedes_every_exclusive_shared_name(feature_app, monkeypatch):
    client, app, _, catalog, _, alice, *_ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        user = (await NativeFileSources.prepare(native_request(app), actor)).user
    company = next(volume for volume in catalog.verified.values() if volume.spec.kind == "company")
    name = "résumé.tar.gz"
    folder = company.data_path / "shared/team"
    folder.mkdir()
    foreign = folder / name
    foreign.write_bytes(b"someone else's file")
    content = b"this publication"
    (user.data_path / "files" / name).write_bytes(content)
    body = {"path": "/mnt/user-data/files/" + name, "folder": "team"}
    saves, attempts = [], []
    save, publish = WorkflowReceipt.save, QualifiedUploadStage.publish

    def saved(receipt):
        save(receipt)
        saves.append(json.loads(receipt.path.read_bytes())["facts"])

    def attempted(stage, filename, **options):
        metadata = os.fstat(stage.fd)
        path = next(company.control_path.glob("host-workflow-*.json"))
        facts = json.loads(path.read_bytes())["facts"]
        attempts.append((filename, facts, {"device": metadata.st_dev, "inode": metadata.st_ino}))
        return publish(stage, filename, **options)

    monkeypatch.setattr(WorkflowReceipt, "save", saved)
    monkeypatch.setattr(QualifiedUploadStage, "publish", attempted)
    response = await client.post("/api/shared/publish", json=body)
    assert response.status_code == 201, response.text
    record = response.json()
    assert saves[0] == {"publication_order": "fact-before-name"}
    assert [filename for filename, _, _ in attempts] == [name, "résumé_1.tar.gz"]
    assert len(saves) == 4
    for filename, facts, identity in attempts:
        assert facts["publication_identity"] == identity
        assert facts["publication"] == {
            "publication_id": record["publication_id"],
            "path": "team/" + filename,
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "published_by": str(alice.id),
            "from_thread_id": None,
            "from_path": body["path"],
        }
    assert saves[1] == {"publication_order": "fact-before-name", "publication_identity": attempts[0][2]}
    assert foreign.read_bytes() == b"someone else's file"
    assert (folder / "résumé_1.tar.gz").read_bytes() == content


@pytest.mark.asyncio
async def test_ordered_copy_accepts_a_host_created_descriptor(feature_app, monkeypatch):
    _, app, files, _, _, alice, *_ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    operation = uuid4().hex
    origin = {"path": "/conversation-snapshot/transcript", "thread_id": str(uuid4()), "folder": ""}
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        company = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "company")
        requests = {**sources.requests, company.id: (Permission.READ | Permission.EXPORT, company.generation)}
        repo = app.state.shared_publications_repo

        async def publish(volumes):
            volume = volumes[company.id]

            async def admission():
                async with files.registry.admitted(actor=actor, requests=requests, operation_id=(company.id, operation)) as (session, rows):
                    await files._host_write_admission(session, rows, company.id, operation)
                    await sources.guard(session, rows)

            owner = NativeSharedCopy(sources, volume, admission)
            actual_publish = QualifiedUploadStage.publish

            def check_fact(stage, name, **options):
                receipt = json.loads((volume.control_path / f"host-workflow-{operation}.json").read_bytes())
                facts = receipt["facts"]
                assert facts["publication_order"] == FACT_BEFORE_NAME
                assert facts["publication_identity"]["inode"] == os.fstat(stage.fd).st_ino
                assert facts["publication"]["path"] == name
                assert facts["publication"]["from_path"] == receipt["request"]["source"]["path"] == origin["path"]
                assert facts["publication"]["from_thread_id"] == receipt["request"]["source"]["thread_id"] == origin["thread_id"]
                assert facts["publication"]["sha256"] == hashlib.sha256(b"generated").hexdigest()
                return actual_publish(stage, name, **options)

            monkeypatch.setattr(QualifiedUploadStage, "publish", check_fact)
            with TemporaryFile() as stream:
                stream.write(b"generated")
                stream.flush()
                source = DescriptorSource(stream.fileno(), "transcript.md")
                try:
                    stored, record = await copy_and_record(
                        repo,
                        source,
                        name=source.name,
                        folder=origin["folder"],
                        user_id=actor.subject_id,
                        thread_id=origin["thread_id"],
                        source_path=origin["path"],
                        copy=None,
                        remove=None,
                        ordered_copy=owner.copy,
                        rollback_async=owner.rollback,
                    )
                finally:
                    await owner.close()
            assert (volume.data_path / "shared" / stored.path).read_bytes() == b"generated"
            assert record["path"] == "transcript.md"
            assert record["from_path"] == origin["path"] and record["from_thread_id"] == origin["thread_id"]

        await files.run_workflow(
            actor=actor,
            requests=requests,
            destination_id=company.id,
            operation_id=operation,
            request={"action": "feature-workflow", "namespace": "hm.shared", "operation": "publish", "source": origin},
            host_write=True,
            initial_facts={"publication_order": FACT_BEFORE_NAME},
            call=publish,
        )

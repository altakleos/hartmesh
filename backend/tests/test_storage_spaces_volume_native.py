"""No-skip merge tier: actual disk mounts and unprivileged Docker writers.

Default contributor runs skip only this separately qualified Linux tier. Its
dedicated CI job requires the fixture and rejects every skipped test.
"""

from __future__ import annotations

import asyncio
import errno
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from _storage_spaces_native_test_support import prepare_consumer_container

from deerflow.spaces.backings import BackingUnavailable, PreparedVolumeCatalog


@pytest.fixture(scope="module")
def typed_prepared_pool():
    if os.environ.get("HARTMESH_REQUIRE_MANAGED_STORAGE") != "1":
        pytest.skip("Typed pool qualification runs in its mandatory Linux CI job")
    import tempfile

    from app.gateway.storage_init import KINDS, StorageInitializer
    from deerflow.config.storage_spaces_config import StoragePlan

    # UID 1000 must reach the pool through ordinary readable ancestors. The
    # runner's private home is outside pytest's fixture ownership; do not chmod it.
    data_dir = Path(tempfile.mkdtemp(prefix="hartmesh-native-typed-", dir="/tmp"))
    data_dir.chmod(0o755)
    fstab = data_dir / "test-fstab"
    fstab.write_text("# native fixture only\n", encoding="utf-8")
    plan = StoragePlan(users=1, user_gib=1, ai_assistants_per_user=0, ai_assistant_gib=1, ai_employees=0, ai_employee_gib=1, company_gib=1)
    tool = StorageInitializer(data_dir, plan, capacities=dict.fromkeys(KINDS, 64 << 20), fstab_path=fstab)
    with tool.locked(repair_pool=True):
        doc = tool.prepare()
        assert doc["returncode"] == 0, doc
    try:
        yield tool
    finally:
        assert getattr(tool, "_native_bridge_contained", True), "Unconfirmed managed primary; retain its pool"
        with tool.locked():
            doc = tool.quiesce()
            assert doc["returncode"] == 0, doc
        # Remove only after confirmed unmount; uncertain backings stay on disk.
        shutil.rmtree(data_dir)


def test_prepared_pool_has_the_design_owners_and_modes(typed_prepared_pool):
    import stat

    tool = typed_prepared_pool
    with tool.locked():
        inherited = tool.host.run("sh", "-c", 'readlink "/proc/self/fd/$1"', "storage-lock-proof", str(tool.host.lock_fd))
        assert inherited.strip() == str(tool.pool / ".storage-init.lock")
    assert (tool.pool.stat().st_uid, tool.pool.stat().st_gid, stat.S_IMODE(tool.pool.stat().st_mode)) == (0, 1000, 0o750)
    assert (tool.manifest.stat().st_uid, tool.manifest.stat().st_gid, stat.S_IMODE(tool.manifest.stat().st_mode)) == (1000, 1000, 0o440)
    assert (tool.pool / ".probe").stat().st_uid == 1000
    inventory = tool.load()
    assert [s.kind for s in inventory.volumes.values()] == ["company", "user"]
    for spec in inventory.volumes.values():
        assert (spec.image_path.stat().st_uid, spec.image_path.stat().st_gid, stat.S_IMODE(spec.image_path.stat().st_mode)) == (0, 1000, 0o640)
        parent = spec.mount_path.stat()
        assert (parent.st_uid, parent.st_gid, stat.S_IMODE(parent.st_mode)) == (0, 1000, 0o770)
        assert spec.image_path.stat().st_blocks * 512 >= 64 << 20
        for name, mode in (("data", 0o770), ("control", 0o700)):
            meta = (spec.mount_path / name).stat()
            assert (meta.st_uid, meta.st_gid, stat.S_IMODE(meta.st_mode)) == (1000, 1000, mode)
        inventory.verify(spec.slot_id)


def test_gateway_identity_can_rename_private_siblings(typed_prepared_pool):
    spec = next(spec for spec in typed_prepared_pool.load().volumes.values() if spec.kind == "user")
    root = os.open(spec.mount_path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    first, second = ".parent-proof-" + uuid.uuid4().hex, ".parent-proof-" + uuid.uuid4().hex
    try:
        child = os.fork()
        if child == 0:
            try:
                os.setgroups([])
                os.setgid(1000)
                os.setuid(1000)
                assert os.getuid() == os.getgid() == 1000
                os.mkdir(first, mode=0o700, dir_fd=root)
                os.rename(first, second, src_dir_fd=root, dst_dir_fd=root)
                os.rmdir(second, dir_fd=root)
            except BaseException:
                os._exit(1)
            os._exit(0)
        _, status = os.waitpid(child, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert not (spec.mount_path / first).exists() and not (spec.mount_path / second).exists()
    finally:
        os.close(root)


@pytest.mark.asyncio
async def test_resized_space_keeps_its_files_and_reports_the_new_bound(typed_prepared_pool, tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.persistence.base import Base
    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
    from deerflow.spaces.contract import Custody, MutationMode, PrincipalRef, ResolvedPrincipal
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    tool = typed_prepared_pool
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'growth.db'}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all, tables=[row.__table__ for row in (SpaceRow, SpaceGrantRow, SpaceEventRow, SpaceBackingRow, StorageFeatureLinkRow)])
        sf = async_sessionmaker(engine, expire_on_commit=False)
        actor = PrincipalRef("human", "native-fixture")

        async def human(reference):
            return ResolvedPrincipal(actor) if reference == actor else None

        registry = SpaceRegistry(sf, HostPrincipalResolver(human=human))
        files = SpaceFiles(registry, tool.load())
        space_id = await files.assign(kind="user", actor=actor, name="Native fixture", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
        spec = next(s for s in files.catalog.volumes.values() if s.kind == "user")
        before = files.catalog.verify(spec.slot_id)
        for root, names in ((before.data_path, ("files", "projects", "conversations")), (before.control_path, ("my-files", "project-staging", "project-conversion"))):
            for name in names:
                meta = (root / name).stat()
                assert (meta.st_uid, meta.st_gid, meta.st_mode & 0o777) == (1000, 1000, 0o700)
        source = spec.mount_path / "data/files/persistent.txt"
        source.write_bytes(b"retained across offline ext4 growth")
        with tool.locked():
            doc = tool.resize("1", slot=spec.slot_id, capacity_bytes=128 << 20)
            assert doc["returncode"] == 0, doc
        grown = tool.load().verify(spec.slot_id)
        assert grown.spec.filesystem_uuid == spec.filesystem_uuid and grown.spec.kind == "user"
        assert (grown.root_inode, grown.control_inode) == (before.root_inode, before.control_inode)
        assert grown.spec.max_bytes == 128 << 20 and grown.spec.max_inodes == (128 << 20) // 8192
        assert grown.available_bytes > 64 << 20
        restarted = SpaceFiles(registry, tool.load())
        await restarted.load_bound_slots()
        assert await restarted.refresh_bound_volumes() == {}
        async with sf() as session:
            space = await session.get(SpaceRow, space_id)
            binding = await session.get(SpaceBackingRow, space.backing_handle)
            assert (binding.max_bytes, binding.max_inodes) == (grown.spec.max_bytes, grown.spec.max_inodes)
        assert source.read_bytes() == b"retained across offline ext4 growth"
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def resident_runtime(resident_backing, tmp_path, monkeypatch):
    """Real resident path on a generic native backing, not managed-v5 proof."""
    import atexit
    from types import SimpleNamespace

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.agent_instances.contract import DefinitionSnapshot, InstanceIdentity
    from deerflow.agent_instances.conversations import AgentConversations
    from deerflow.agent_instances.directory import InstanceDirectory
    from deerflow.agent_instances.residents import ResidentRegistry
    from deerflow.agent_instances.runtime import qualified_attachments
    from deerflow.agent_instances.service import AgentInstances
    from deerflow.agent_instances.work import AgentWork
    from deerflow.agent_instances.work_execution import WorkExecution
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.config.app_config import AppConfig, pop_current_app_config, push_current_app_config
    from deerflow.config.sandbox_config import SandboxConfig
    from deerflow.persistence import models  # noqa: F401 -- register complete ORM
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.base import Base
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.spaces.contract import PrincipalRef, ResolvedPrincipal
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles
    from deerflow.utils.file_io import await_drained, run_file_io

    image = os.environ.get("HARTMESH_INSTANCE_AIO_IMAGE")
    assert image and "@sha256:" in image, "Native instance qualification requires the pinned shipped AIO image"
    if writer_runtime := os.environ.get("HARTMESH_NATIVE_WRITER_RUNTIME"):
        monkeypatch.setenv("DEER_FLOW_SANDBOX_RUNTIME", writer_runtime)
    for key, value in {"DEER_FLOW_SANDBOX_IMAGE_STARTUP_CAPS": "0", "DEER_FLOW_SANDBOX_MEMORY": "768m", "DEER_FLOW_SANDBOX_CPUS": "1", "DEER_FLOW_SANDBOX_PIDS_LIMIT": "128", "DEER_FLOW_SANDBOX_CONTAINER_USER": "1000:1000"}.items():
        monkeypatch.setenv(key, value)
    catalog, _, _, containment = resident_backing
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'agent-native.db'}")
    baseline = residents = files = instance = None
    config_pushed = False
    clients = []
    alice, bob = PrincipalRef("human", "alice"), PrincipalRef("human", "bob")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sf = async_sessionmaker(engine, expire_on_commit=False)

        async def human(reference):
            return ResolvedPrincipal(reference, reference == alice) if reference in (alice, bob) else None

        directory = InstanceDirectory(sf, human=human)
        files = SpaceFiles(SpaceRegistry(sf, HostPrincipalResolver(human=human, nonhuman=directory.lookup)), catalog, inventory_path=catalog.data_disk / "inventory.v1.json")
        agents = AgentInstances(files, directory)
        authority = AgentConversations(agents)
        instance = await agents.create(
            actor=alice,
            creation_id=uuid.uuid4().hex,
            name="Qualification AI employee",
            custody="company",
            supervisor=bob,
            definition=DefinitionSnapshot.capture(owner_id="alice", config={"name": "analyst", "skills": [], "mcp_plugins": []}, soul="Analyze team work."),
        )
        app_config = AppConfig(
            models=[],
            sandbox=SandboxConfig(
                use="deerflow.community.aio_sandbox:AioSandboxProvider",
                image=image,
                replicas=2,
                idle_timeout=0,
                container_prefix="hartmesh-agent-native-" + uuid.uuid4().hex,
                ownership={"type": "memory"},
                environment={key: "true" for key in ("DISABLE_BROWSER", "DISABLE_JUPYTER", "DISABLE_CODE_SERVER", "DISABLE_VNC", "DISABLE_MCP_BROWSER", "DISABLE_NODEJS_REPL")},
            ),
        )
        # Scope configuration only; constructor, provider state and all native
        # preparation/containment operations remain their real implementations.
        push_current_app_config(app_config)
        config_pushed = True
        baseline = await await_drained(run_file_io(AioSandboxProvider))
        _provider, attachments = await qualified_attachments(files, baseline_provider=baseline)

        async def owner_of(home_id):
            async with sf() as session:
                row = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.home_id == home_id))
                return InstanceIdentity.from_row(row) if row is not None else None

        async def settle(**kwargs):
            return await WorkExecution(AgentWork(agents)).settle_contained(host_id=attachments.provider.host_id, **kwargs)

        residents = ResidentRegistry(attachments=attachments, provider=baseline, run_manager=RunManager(), settle=settle, owner_of=owner_of, replicas=2, idle_timeout=0)
        agents.residents = residents
        attachments.on_contained = residents.contained

        def install_accounting():
            baseline.external_slots = residents.external_slots
            baseline.external_native_claims = residents.external_native_claims
            baseline.evict_external = residents.evict_external

        baseline.external_changed(install_accounting)
        token = residents.begin_restore()
        residents.finish_restore(token, await attachments.retained_claims())
        yield SimpleNamespace(
            agents=agents,
            authority=authority,
            files=files,
            instance=instance,
            alice=alice,
            bob=bob,
            baseline=baseline,
            app_config=app_config,
            containment=containment,
            residents=residents,
            sf=sf,
            clients=clients,
            threads=ThreadMetaRepository(sf, instance_authority=authority),
        )
    finally:

        async def cleanup():
            errors = []
            for client in clients:
                try:
                    await await_drained(run_file_io(client.close))
                except BaseException as error:
                    errors.append(error)
            try:
                if files is not None and instance is not None and files.attachments is not None:
                    home = await files.registry.get(actor=alice, space_id=instance.home_id)
                    await files.attachments.retire(reason="storage-recovery", actor=alice, space_id=home.id, expected_generation=home.generation)
                    containment["confirmed"] = True
            except BaseException as error:
                errors.append(error)
            if residents is not None:
                try:
                    await await_drained(residents.close())
                except BaseException as error:
                    errors.append(error)
            if baseline is not None:
                try:
                    await await_drained(run_file_io(baseline.shutdown))
                    atexit.unregister(baseline.shutdown)
                except BaseException as error:
                    errors.append(error)
            try:
                await engine.dispose()
            except BaseException as error:
                errors.append(error)
            if errors:
                raise BaseExceptionGroup("Native resident fixture cleanup failed", errors)

        try:
            await await_drained(cleanup())
        finally:
            if config_pushed:
                pop_current_app_config()


async def native_prepare(runtime, actor, thread_id):
    from deerflow.agent_instances.runtime import execution_scope, prepare_environment
    from deerflow.config.app_config import pop_current_app_config, push_current_app_config

    execution = await runtime.authority.execution(actor=actor, thread_id=thread_id)
    runtime.containment["confirmed"] = False
    push_current_app_config(runtime.app_config)
    try:
        with execution_scope(execution) as environment:
            try:
                provider = await prepare_environment(execution, baseline_provider=runtime.baseline, app_config=runtime.app_config)
                assert environment.provider is provider and environment.attachment_id == provider.sandbox.id
            finally:
                if environment.provider is not None:
                    runtime.clients.append(environment.provider)
    finally:
        pop_current_app_config()
    return provider


@pytest.mark.asyncio
async def test_agent_two_chats_use_one_qualified_home_with_real_aio_protocol(resident_runtime):
    """Use the shipped AIO image through real resident admission and the SDK."""
    from deerflow.agent_instances.lifecycle import InstanceLifecycle
    from deerflow.utils.file_io import await_drained, run_file_io

    runtime = resident_runtime
    agents, files, authority, threads = runtime.agents, runtime.files, runtime.authority, runtime.threads
    instance, alice, bob, containment = runtime.instance, runtime.alice, runtime.bob, runtime.containment
    first = second = third = None
    containment["confirmed"] = False
    try:
        one = await authority.create(actor=alice, instance_id=instance.id, thread_id=uuid.uuid4().hex)
        first = await native_prepare(runtime, alice, one["thread_id"])
        await await_drained(
            run_file_io(
                first.sandbox.execute_command,
                "mkdir -p /mnt/spaces/home/wiki /mnt/spaces/home/repo"
                " && printf '# Team wiki\\n' > /mnt/spaces/home/wiki/home.md"
                " && git -C /mnt/spaces/home/repo init --quiet"
                " && python3 -c \"import sqlite3; c=sqlite3.connect('/mnt/spaces/home/catalog.sqlite'); "
                "c.execute('create table work (value text)'); c.execute('insert into work values (?)', ('retained',)); c.commit(); c.close()\"",
            )
        )
        # Exercise physical Home cwd, then same-call presentation against the
        # qualified native backing rather than a Gateway /mnt pathname.
        import time
        from types import SimpleNamespace

        from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
        from deerflow.agent_instances.runtime import execution_scope
        from deerflow.tools.presentation import validate_presentation

        started = time.time()
        creation = await await_drained(
            run_file_io(
                first.sandbox.execute_command,
                "cd /mnt/user-data/workspace && python3 -c \"import os; print('physical cwd:', os.getcwd()); assert os.getcwd() == '/mnt/spaces/home'; "
                "open('uploads/input.txt','w').write('input'); open('outputs/result.txt','w').write('native output'); print('HARTMESH_OUTPUT_CREATED')\"",
            )
        )
        assert "HARTMESH_OUTPUT_CREATED" in creation.splitlines(), creation
        assert await await_drained(run_file_io(first.sandbox.read_file, "/mnt/spaces/home/outputs/result.txt")) == "native output"
        with execution_scope(first.execution) as environment:
            environment.provider = first
            presentation_runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: first.execution, "thread_id": first.execution.thread_id}, state={"thread_data": first.execution.thread_paths})
            presentation = await await_drained(asyncio.to_thread(validate_presentation, presentation_runtime, ["/mnt/user-data/outputs/result.txt"], written_after=started))
        assert presentation.presented == ["/mnt/user-data/outputs/result.txt"], presentation.refused
        assert presentation.sizes == {"/mnt/user-data/outputs/result.txt": 13}
        # Run the actual three consumer writers on qualified Home. These are
        # test-owned executable copies, not evidence of private-skill admission.
        import shlex

        consumer_settings = [
            ("reporting", "skills/public/business-report/scripts/business_report_settings.py", "summary_length", "short", "preferences"),
            ("operations", "examples/skills/procedure-summary/scripts/working_data.py", "include_caveats", False, "settings"),
            ("vendor", "examples/skills/supplier-comparison/scripts/working_data.py", "show_delivery", False, "settings"),
        ]
        consumer_revisions = {}
        await await_drained(run_file_io(first.sandbox.execute_command, "mkdir -p /mnt/spaces/home/qualification"))
        for consumer, source, field, value, key in consumer_settings:
            code = (Path(__file__).resolve().parents[2] / source).read_text(encoding="utf-8")
            await await_drained(run_file_io(first.sandbox.write_file, f"/mnt/spaces/home/qualification/{consumer}.py", code))
            payload = json.dumps({"version": 1, field: value})
            await await_drained(run_file_io(first.sandbox.write_file, f"/mnt/spaces/home/qualification/{consumer}-request.json", payload))
            command = f"python3 /mnt/spaces/home/qualification/{consumer}.py save /mnt/spaces/home/workflow-data/{consumer}/settings.json --from /mnt/spaces/home/qualification/{consumer}-request.json --expected missing --operation initial"
            output = await await_drained(run_file_io(first.sandbox.execute_command, command))
            receipt = next(json.loads(line) for line in output.splitlines() if line.startswith('{"'))
            assert receipt[key][field] == value
            consumer_revisions[consumer] = receipt["revision"]
        await await_drained(run_file_io(first.close))
        two = await authority.create(actor=bob, instance_id=instance.id, thread_id=uuid.uuid4().hex)
        second = await native_prepare(runtime, bob, two["thread_id"])
        assert first.sandbox.id == second.sandbox.id
        for consumer, _, field, value, key in consumer_settings:
            output = await await_drained(run_file_io(second.sandbox.execute_command, f"python3 /mnt/spaces/home/qualification/{consumer}.py read /mnt/spaces/home/workflow-data/{consumer}/settings.json"))
            receipt = next(json.loads(line) for line in output.splitlines() if line.startswith('{"'))
            assert receipt["revision"] == consumer_revisions[consumer] and receipt[key][field] == value
        assert await await_drained(run_file_io(second.sandbox.read_file, "/mnt/spaces/home/wiki/home.md")) == "# Team wiki\n"
        db = await await_drained(run_file_io(second.sandbox.execute_command, "python3 -c \"import sqlite3; print(sqlite3.connect('/mnt/spaces/home/catalog.sqlite').execute('select value from work').fetchone()[0])\""))
        assert "retained" in db
        assert "ref:" in await await_drained(run_file_io(second.sandbox.read_file, "/mnt/spaces/home/repo/.git/HEAD"))
        await threads.delete(one["thread_id"], user_id="alice")
        assert (await agents.get(actor=bob, instance_id=instance.id)).home_id == instance.home_id
        assert "# Team wiki" in await await_drained(run_file_io(second.sandbox.read_file, "/mnt/spaces/home/wiki/home.md"))
        # Storage owns containment; SDK closure and chat deletion leave home intact.
        await await_drained(run_file_io(second.close))
        lifecycle = InstanceLifecycle(agents)
        stopped = await lifecycle.change(actor=alice, instance_id=instance.id, expected_generation=instance.generation, operation_id=uuid.uuid4().hex, action="suspend")
        assert stopped["complete"] and stopped["instance"].status == "suspended"
        assert await files.read(actor=alice, space_id=instance.home_id, path="wiki/home.md", max_bytes=1024) == b"# Team wiki\n"
        restored = await lifecycle.change(actor=alice, instance_id=instance.id, expected_generation=stopped["instance"].generation, operation_id=uuid.uuid4().hex, action="restore")
        assert restored["complete"] and restored["instance"].home_id == instance.home_id
        third = await native_prepare(runtime, bob, two["thread_id"])
        assert third.sandbox.id != second.sandbox.id
        for consumer, _, _, _, key in consumer_settings:
            command = f"python3 /mnt/spaces/home/qualification/{consumer}.py reset /mnt/spaces/home/workflow-data/{consumer}/settings.json --expected {shlex.quote(consumer_revisions[consumer])} --operation reset-after-replacement"
            output = await await_drained(run_file_io(third.sandbox.execute_command, command))
            receipt = next(json.loads(line) for line in output.splitlines() if line.startswith('{"'))
            assert receipt[key] == {"version": 1} and receipt["revision"] != consumer_revisions[consumer]
        assert await await_drained(run_file_io(third.sandbox.read_file, "/mnt/spaces/home/wiki/home.md")) == "# Team wiki\n"
        assert "retained" in await await_drained(run_file_io(third.sandbox.execute_command, "python3 -c \"import sqlite3; print(sqlite3.connect('/mnt/spaces/home/catalog.sqlite').execute('select value from work').fetchone()[0])\""))
        await await_drained(run_file_io(third.close))
        home = await files.registry.get(actor=alice, space_id=instance.home_id)
        await files.attachments.retire(reason="storage-recovery", actor=alice, space_id=home.id, expected_generation=home.generation)
        containment["confirmed"] = True
    finally:
        if first is not None:
            await await_drained(run_file_io(first.close))
        if second is not None:
            await await_drained(run_file_io(second.close))
        if third is not None:
            await await_drained(run_file_io(third.close))
        if not containment["confirmed"] and getattr(files, "attachments", None) is not None:
            home = await files.registry.get(actor=alice, space_id=instance.home_id)
            await files.attachments.retire(reason="storage-recovery", actor=alice, space_id=home.id, expected_generation=home.generation)
            containment["confirmed"] = True


@pytest.mark.asyncio
async def test_host_containment_removes_the_real_container_and_the_next_run_attaches_afresh(resident_runtime):
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
    from deerflow.utils.file_io import await_drained, run_file_io

    runtime = resident_runtime
    conversation = await runtime.authority.create(actor=runtime.alice, instance_id=runtime.instance.id, thread_id=uuid.uuid4().hex)
    first = await native_prepare(runtime, runtime.alice, conversation["thread_id"])
    runtime.containment["confirmed"] = False
    attachments = runtime.files.attachments
    identity = await attachments.retained_attachment(first.sandbox.id)
    assert identity is not None and identity.container_id is not None
    await await_drained(run_file_io(first.sandbox.write_file, "/mnt/spaces/home/outputs/retained.txt", "retained through host containment"))
    await await_drained(run_file_io(first.close))
    assert await attachments.contain_host(identity.id, "operator")
    # SQL liveness reads only active attachments. After fencing, prove physical
    # absence by the captured immutable Docker identity instead.
    result = subprocess.run(["docker", "inspect", identity.container_id], capture_output=True, text=True, timeout=30)
    assert result.returncode == 1 and result.stdout.strip() in ("", "[]")
    assert result.stderr.strip() in ("Error: No such object: " + identity.container_id, "error: no such object: " + identity.container_id, "Error response from daemon: No such container: " + identity.container_id)
    async with runtime.sf() as session:
        row = await session.get(SpaceAttachmentRow, identity.id)
        assert row.phase == "fenced" and row.fenced_by_kind == "host" and row.fenced_by_id == attachments.provider.host_id
        assert row.fence_reason == "operator" and row.fenced_at is not None
    assert identity.id not in runtime.residents.retained and runtime.residents.external_slots() == 0
    second = await native_prepare(runtime, runtime.alice, conversation["thread_id"])
    replacement = await attachments.retained_attachment(second.sandbox.id)
    assert replacement.id != identity.id and replacement.container_id != identity.container_id
    assert await await_drained(run_file_io(second.sandbox.read_file, "/mnt/spaces/home/outputs/retained.txt")) == "retained through host containment"


@pytest.mark.asyncio
async def test_real_attachment_fences_children_and_replaces_environment(backing, tmp_path):
    """A real container can span calls; exact removal stops every mounted writer."""
    import sqlite3

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.persistence.base import Base
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceBackupRow, SpaceMountRow
    from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
    from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
    from deerflow.spaces.contract import Custody, MutationMode, PrincipalRef, ResolvedPrincipal
    from deerflow.spaces.docker import ATTACHMENT_LABEL, HOST_LABEL, DockerStorageAdapter
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.recovery import SpaceRecovery
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    catalog, volume, image, containment = backing
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'native-authority.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[m.__table__ for m in (SpaceRow, SpaceGrantRow, SpaceEventRow, SpaceBackingRow, SpaceFileOperationRow, SpaceAttachmentRow, SpaceMountRow, SpaceBackupRow)])
    actor = PrincipalRef("human", "native-fixture")

    async def lookup(reference):
        return ResolvedPrincipal(reference) if reference == actor else None

    files = SpaceFiles(SpaceRegistry(async_sessionmaker(engine, expire_on_commit=False), HostPrincipalResolver(human=lookup)), catalog)
    containers = []

    def prepare(plan):
        args = [
            "docker",
            "create",
            "--pull=never",
            "--name",
            "hartmesh-attachment-" + plan.id,
            "--label",
            ATTACHMENT_LABEL + "=" + plan.id,
            "--label",
            HOST_LABEL + "=" + plan.host_id,
            "--read-only",
            "--user",
            "1000:1000",
            "--cap-drop=ALL",
            "--security-opt",
            "no-new-privileges",
            "--network",
            "none",
            "--memory",
            "128m",
            "--pids-limit",
            "64",
        ]
        writer_runtime = os.environ.get("HARTMESH_NATIVE_WRITER_RUNTIME")
        if writer_runtime:
            index = args.index("--pull=never") + 1
            args[index:index] = ["--runtime", writer_runtime]
        for view in plan.views:
            args += ["--mount", "type=bind,src=" + view.source + ",dst=" + view.destination + ("" if view.writable else ",readonly")]
        program = """import os,time,sqlite3
root='/mnt/spaces/home'
db=sqlite3.connect(root+'/pages.sqlite')
db.execute('PRAGMA journal_mode=WAL')
db.execute('CREATE TABLE IF NOT EXISTS pages (body TEXT)')
db.execute("INSERT INTO pages VALUES ('committed')")
db.commit()
if os.fork()==0:
    while True:
        with open(root+'/child','ab') as f: f.write(b'x'); f.flush(); os.fsync(f.fileno())
        time.sleep(.02)
while True: time.sleep(.1)
"""
        if len(containers) == 1:
            program = "import pathlib,time; pathlib.Path('/mnt/spaces/home/peer').write_text('ready');\nwhile True: time.sleep(.1)"
        result = subprocess.run([*args, image, "python", "-c", program], capture_output=True, text=True, check=True, timeout=30)
        containers.append((result.stdout.strip(), plan.id))
        return result.stdout.strip()

    provider = DockerStorageAdapter(prepare=prepare, pool=catalog.data_disk, probe_image=os.environ["HARTMESH_INSTANCE_AIO_IMAGE"])
    mounts = SpaceAttachments(files, provider)
    recovery = SpaceRecovery(files, attachments=mounts)
    try:
        space = await files.create(actor=actor, name="native", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
        await mounts.attach(actor=actor, incarnation=uuid.uuid4().hex, resources=[ResourceMount(space.id, 1, "home", writable=True)])
        deadline = asyncio.get_running_loop().time() + 15
        while not (volume.data_path / "child").exists():
            assert asyncio.get_running_loop().time() < deadline, "Native child did not begin writing"
            await asyncio.sleep(0.05)
        await mounts.attach(actor=actor, incarnation=uuid.uuid4().hex, resources=[ResourceMount(space.id, 1, "home", writable=True)])
        deadline = asyncio.get_running_loop().time() + 15
        while not (volume.data_path / "peer").exists():
            assert asyncio.get_running_loop().time() < deadline, "Parallel attachment did not write"
            await asyncio.sleep(0.05)
        await files.write(actor=actor, space_id=space.id, expected_generation=1, operation_id=uuid.uuid4().hex, path="host", content=b"host beside two containers", create=True)
        backup = await recovery.backup(actor=actor, space_id=space.id, expected_generation=1, operation_id=uuid.uuid4().hex)
        before = (volume.data_path / "child").read_bytes()
        await asyncio.sleep(0.2)
        assert (volume.data_path / "child").read_bytes() == before
        assert (volume.data_path / "pages.sqlite-wal").exists(), "Fixture must exercise interrupted SQLite WAL"
        await recovery.restore(actor=actor, space_id=space.id, expected_generation=1, operation_id=uuid.uuid4().hex, backup_id=backup.id)
        with sqlite3.connect(volume.data_path / "pages.sqlite") as restored:
            assert restored.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert restored.execute("SELECT body FROM pages").fetchall() == [("committed",)]
        # A new environment sees persisted files under the same resource ID.
        await mounts.attach(actor=actor, incarnation=uuid.uuid4().hex, resources=[ResourceMount(space.id, 2, "home", writable=True)])
        await mounts.retire(reason="storage-recovery", actor=actor, space_id=space.id, expected_generation=2)
    finally:
        try:
            for container, attachment_id in containers:
                provider.fence(container, attachment_id)
        except Exception:
            containment["confirmed"] = False
            raise
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_qualified_company_wiki_nonhuman_actor_survives_native_replacement(backing, tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.persistence.base import Base
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceBackupRow, SpaceMountRow
    from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
    from deerflow.runtime import user_context
    from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
    from deerflow.spaces.contract import Permission, PrincipalRef, ResolvedPrincipal, SpaceDenied
    from deerflow.spaces.docker import DockerStorageAdapter
    from deerflow.spaces.facade import HostStorageProvider, storage_actor_scope
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    catalog, volume, image, containment = backing
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'consumer-authority.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[m.__table__ for m in (SpaceRow, SpaceGrantRow, SpaceEventRow, SpaceBackingRow, SpaceFileOperationRow, SpaceAttachmentRow, SpaceMountRow, SpaceBackupRow)])
    owner, worker, outsider = PrincipalRef("human", "wiki-owner"), PrincipalRef("nonhuman", "worker:wiki"), PrincipalRef("human", "outsider")
    active = {owner: ResolvedPrincipal(owner, True), worker: ResolvedPrincipal(worker), outsider: ResolvedPrincipal(outsider)}

    async def lookup(reference):
        return active.get(reference)

    files = SpaceFiles(SpaceRegistry(async_sessionmaker(engine, expire_on_commit=False), HostPrincipalResolver(human=lookup, nonhuman=lookup)), catalog)
    provider = HostStorageProvider(lambda: files)
    containers = []
    program = [""]

    def prepare(plan):
        return prepare_consumer_container(plan, image, program[0], containers, containment)

    docker = DockerStorageAdapter(prepare=prepare, pool=catalog.data_disk, probe_image=os.environ["HARTMESH_INSTANCE_AIO_IMAGE"])
    attachments = SpaceAttachments(files, docker)
    token = user_context._current_user.set(None)
    try:
        with storage_actor_scope(owner):
            administrator = await provider.current()
            resource = await administrator.provision(name="Restricted wiki", custody="company")
            from deerflow_extension_api import StorageActor

            resource = await administrator.grant(
                space_id=resource.id, generation=1, subject=StorageActor("nonhuman", worker.subject_id), permissions=int(Permission.READ | Permission.WRITE | Permission.EXPORT), acknowledge_existing_data=True
            )
        with storage_actor_scope(outsider), pytest.raises(SpaceDenied):
            await (await provider.current()).get(space_id=resource.id)
        with storage_actor_scope(worker):
            assert user_context.get_current_user() is None
            consumer = await provider.current()
            assert consumer.actor.kind == "nonhuman" and consumer.capabilities.actor_kinds == ("human", "nonhuman")
            await consumer.mkdir(space_id=resource.id, generation=resource.generation, operation_id=uuid.uuid4().hex, path="pages")
            revision = await consumer.write(space_id=resource.id, generation=resource.generation, operation_id=uuid.uuid4().hex, path="pages/home.md", content=b"# Home\nFirst\n", create=True)
            await consumer.write(space_id=resource.id, generation=resource.generation, operation_id=uuid.uuid4().hex, path="pages/home.md", content=b"# Home\nSecond\n", expected_sha256=revision)
            source_reference = {"space_id": resource.id, "path": "pages/home.md"}
            await consumer.write(space_id=resource.id, generation=resource.generation, operation_id=uuid.uuid4().hex, path="sources.json", content=json.dumps(source_reference).encode("utf-8"), create=True)
            script = b"""import pathlib,sqlite3
root=pathlib.Path(__file__).parent
db=sqlite3.connect(root/'index.sqlite')
db.execute('PRAGMA journal_mode=WAL')
db.execute('CREATE TABLE IF NOT EXISTS pages (path TEXT PRIMARY KEY, body TEXT)')
for page in (root/'pages').glob('*.md'):
    db.execute('INSERT OR REPLACE INTO pages VALUES (?,?)',(page.name,page.read_text()))
db.commit(); db.close()
"""
            await consumer.write(space_id=resource.id, generation=resource.generation, operation_id=uuid.uuid4().hex, path="index_pages.py", content=script, create=True)
        program[0] = "import runpy,time,pathlib; root=pathlib.Path('/mnt/spaces/wiki'); runpy.run_path(str(root/'index_pages.py')); (root/'indexed').write_text('ready');\nwhile True: time.sleep(.1)"
        attachment = await attachments.attach(actor=worker, incarnation=uuid.uuid4().hex, resources=[ResourceMount(resource.id, resource.generation, "wiki", writable=True)])
        deadline = asyncio.get_running_loop().time() + 15
        while not (volume.data_path / "indexed").exists():
            assert asyncio.get_running_loop().time() < deadline, "Native wiki index did not become ready"
            await asyncio.sleep(0.05)
        with storage_actor_scope(worker):
            await consumer.write(space_id=resource.id, generation=resource.generation, operation_id=uuid.uuid4().hex, path="host-edit", content=b"x", create=True)
        assert (volume.data_path / "host-edit").read_bytes() == b"x"
        await attachments.retire(reason="storage-recovery", actor=owner, space_id=resource.id, expected_generation=resource.generation, attachment_ids=[attachment.id])
        program[0] = (
            "import sqlite3,json,time,pathlib; root=pathlib.Path('/mnt/spaces/wiki'); db=sqlite3.connect(root/'index.sqlite'); "
            "body=db.execute('SELECT body FROM pages WHERE path=?',('home.md',)).fetchone()[0]; db.close(); "
            "(root/'replacement.json').write_text(json.dumps({'body':body,'source':json.loads((root/'sources.json').read_text())}));\nwhile True: time.sleep(.1)"
        )
        replacement = await attachments.attach(actor=worker, incarnation=uuid.uuid4().hex, resources=[ResourceMount(resource.id, resource.generation, "wiki", writable=True)])
        deadline = asyncio.get_running_loop().time() + 15
        while not (volume.data_path / "replacement.json").exists():
            assert asyncio.get_running_loop().time() < deadline, "Replacement did not read retained wiki data"
            await asyncio.sleep(0.05)
        await attachments.retire(reason="storage-recovery", actor=owner, space_id=resource.id, expected_generation=resource.generation, attachment_ids=[replacement.id])
        with storage_actor_scope(worker):
            independent = await provider.current()
            assert json.loads(await independent.read(space_id=resource.id, path="replacement.json", max_bytes=1000)) == {"body": "# Home\nSecond\n", "source": source_reference}
        del active[worker]
        from deerflow_extension_api import StorageIdentityRequired

        with storage_actor_scope(worker), pytest.raises(StorageIdentityRequired):
            await consumer.read(space_id=resource.id, path="pages/home.md", max_bytes=100)
        with storage_actor_scope(owner):
            retained = await administrator.get(space_id=resource.id)
            assert retained.id == resource.id and retained.custody == "company" and retained.custodian is None
            assert await administrator.read(space_id=resource.id, path="pages/home.md", max_bytes=100) == b"# Home\nSecond\n"
    finally:
        user_context._current_user.reset(token)
        try:
            for container, attachment_id in containers:
                docker.fence(container, attachment_id)
        except Exception:
            containment["confirmed"] = False
            raise
        finally:
            await engine.dispose()


def test_real_create_with_lost_acknowledgement_preserves_backing_until_verified_absent(backing):
    from deerflow.spaces.attachments import AttachmentPlan, NativeView
    from deerflow.spaces.contract import PrincipalRef
    from deerflow.spaces.docker import DockerStorageAdapter

    catalog, volume, image, containment = backing
    adapter = DockerStorageAdapter(prepare=lambda _plan: pytest.fail("This test creates only through its owned fixture helper"), pool=catalog.data_disk, probe_image=os.environ["HARTMESH_INSTANCE_AIO_IMAGE"])
    plan = AttachmentPlan(uuid.uuid4().hex, uuid.uuid4().hex, PrincipalRef("nonhuman", "worker:fixture"), adapter.host_id, (NativeView(uuid.uuid4().hex, 1, str(volume.data_path), "/mnt/spaces/wiki", True),))
    tracked, created = [], []

    def real_create_then_error(arguments, **kwargs):
        result = subprocess.run(arguments, **kwargs)
        created.append(result.stdout.strip())
        raise subprocess.TimeoutExpired(arguments, 30)

    try:
        with pytest.raises(subprocess.TimeoutExpired):
            prepare_consumer_container(plan, image, "pass", tracked, containment, runner=real_create_then_error)
        assert created and not tracked and containment["confirmed"] is False
        inspection = subprocess.run(["docker", "inspect", "--format", "{{.State.Status}}", created[0]], check=True, capture_output=True, text=True, timeout=15)
        assert inspection.stdout.strip() == "created"
    finally:
        for identity in created:
            adapter.fence(identity, plan.id)
        if created:
            containment["confirmed"] = True


def _prepared_native(tmp_path_factory, *, inodes):
    if os.environ.get("HARTMESH_REQUIRE_MANAGED_STORAGE") != "1":
        pytest.skip("Managed-disk qualification runs in its mandatory Linux CI job")
    if os.geteuid() != 0 or not shutil.which("docker"):
        pytest.fail("Mandatory managed-storage tier requires real mount authority and Docker")
    image = os.environ.get("HARTMESH_STORAGE_WRITER_IMAGE")
    if not image:
        pytest.fail("Mandatory native writer image was not provided")
    script = Path(__file__).resolve().parents[2] / "scripts" / "storage_spaces_volume.py"
    spec = importlib.util.spec_from_file_location("native_storage_operator", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    disk = tmp_path_factory.mktemp("resource volume")
    volumes = module.prepare_volumes(disk, count=1, size_bytes=64 << 20, inodes=inodes, reserve_bytes=16 << 20, reserve_inodes=64, data_uid=1000)
    catalog = PreparedVolumeCatalog.from_manifest(disk / "inventory.v1.json")
    probe = disk / ".probe"
    probe.mkdir(mode=0o700)
    os.chown(probe, 1000, 1000)
    containment = {"confirmed": True}
    try:
        yield catalog, volumes[0], image, containment
    finally:
        # Every native invocation proves its named container absent first.
        if containment["confirmed"]:
            subprocess.run([shutil.which("umount"), str(volumes[0].mount_path)], check=True, timeout=30)


@pytest.fixture(scope="module")
def prepared(tmp_path_factory):
    yield from _prepared_native(tmp_path_factory, inodes=256)


@pytest.fixture(scope="module")
def resident_prepared(tmp_path_factory):
    # Resident admission needs 256 FREE inodes after the Home layout exists.
    yield from _prepared_native(tmp_path_factory, inodes=4096)


@pytest.fixture
def resident_backing(resident_prepared):
    yield from _backing(resident_prepared)


@pytest.fixture
def backing(prepared):
    yield from _backing(prepared)


def _backing(prepared):
    catalog, spec, image, containment = prepared
    if not containment["confirmed"]:
        pytest.fail("Earlier native writer containment is pending; preserve its disk")
    volume = catalog.verify(spec.slot_id)
    try:
        yield catalog, volume, image, containment
    finally:
        # This fixture owns only its new data/control tree, never other slots.
        if containment["confirmed"]:
            for parent in (volume.data_path, volume.control_path):
                for child in parent.iterdir():
                    if child.is_dir() and not child.is_symlink():
                        shutil.rmtree(child)
                    else:
                        child.unlink()


def _native(backing, program: str, *, readonly: bool = False):
    _catalog, volume, image, containment = backing
    name = "hartmesh-storage-native-" + uuid.uuid4().hex
    mount = f"type=bind,src={volume.data_path},dst=/data" + (",readonly" if readonly else "")
    command = [
        "docker",
        "run",
        "--rm",
        "--pull=never",
        "--name",
        name,
        "--label",
        "hartmesh.task=storage-spaces-ci",
        "--read-only",
        "--user",
        "1000:1000",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--network",
        "none",
        "--mount",
        mount,
        image,
        "python3",
        "-c",
        program,
    ]
    writer_runtime = os.environ.get("HARTMESH_NATIVE_WRITER_RUNTIME")
    if writer_runtime:
        index = command.index("--pull=never") + 1
        command[index:index] = ["--runtime", writer_runtime]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
        return json.loads(result.stdout)
    finally:
        containment["confirmed"] = False
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True, timeout=30)
        probe = subprocess.run(["docker", "inspect", name], check=False, capture_output=True, text=True, timeout=15)
        if probe.returncode != 1 or "No such object" not in probe.stderr:
            pytest.fail("Native writer containment could not be confirmed; preserve the backing")
        containment["confirmed"] = True


def test_native_repository_binary_and_database_survive_a_new_container(backing):
    first = _native(
        backing,
        "import os,sqlite3,json; os.mkdir('/data/.git'); open('/data/.git/HEAD','wb').write(b'ref: refs/heads/main\\n'); "
        "open('/data/binary','wb').write(bytes(range(256))); db=sqlite3.connect('/data/wiki.sqlite'); "
        "db.execute('CREATE TABLE pages (title TEXT)'); db.execute('INSERT INTO pages VALUES (?)',('Home',)); "
        "db.commit(); db.close(); print(json.dumps({'created':True}))",
    )
    assert first == {"created": True}
    second = _native(backing, "import sqlite3,json; db=sqlite3.connect('/data/wiki.sqlite'); title=db.execute('SELECT title FROM pages').fetchone()[0]; print(json.dumps({'title':title,'binary':len(open('/data/binary','rb').read())}))")
    assert second == {"title": "Home", "binary": 256}
    with backing[1].filesystem() as fs:
        assert fs.read_bytes(".git/HEAD", max_bytes=100) == b"ref: refs/heads/main\n"


def test_real_git_branches_tests_and_internal_links_on_qualified_backing(backing):
    """Git runs on the qualified host; source edits/tests run in native containers."""
    executable = shutil.which("git")
    assert executable, "Repository qualification requires preinstalled Git"
    volume = backing[1]
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull)

    def git(*arguments):
        return subprocess.run(
            [executable, "-c", "gc.auto=0", "-c", f"safe.directory={volume.data_path}", "-c", "init.templateDir=", "-c", "user.name=Storage fixture", "-c", "user.email=storage@example.test", *arguments],
            cwd=volume.data_path,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()

    git("init", "--initial-branch=main")
    with volume.filesystem() as fs:
        fs.mkdir("src")
        fs.write_atomic("src/value.py", b"VALUE = 1\n", expected_sha256=None, create=True)
        fs.write_atomic("run_tests.py", b"import runpy\nassert runpy.run_path('/data/src/value.py')['VALUE'] == 1\n", expected_sha256=None, create=True)
        fs.write_atomic("binary.bin", bytes(range(256)), expected_sha256=None, create=True)
    os.symlink("src/value.py", volume.data_path / "current.py")
    git("add", ".")
    git("commit", "-m", "Initial fixture")
    initial = git("rev-parse", "HEAD")
    git("switch", "-c", "fixture-edit")
    first = _native(
        backing,
        "import json,runpy; runpy.run_path('/data/run_tests.py'); open('/data/src/value.py','w').write('VALUE = 2\\n'); "
        "open('/data/run_tests.py','w').write(\"import runpy\\nassert runpy.run_path('/data/src/value.py')['VALUE'] == 2\\n\"); "
        "runpy.run_path('/data/run_tests.py'); print(json.dumps({'value':runpy.run_path('/data/current.py')['VALUE']}))",
    )
    assert first == {"value": 2}
    git("add", "src/value.py", "run_tests.py")
    git("commit", "-m", "Native fixture edit")
    changed = git("rev-parse", "HEAD")
    assert changed != initial
    git("switch", "main")
    with volume.filesystem() as fs:
        assert fs.read_bytes("current.py", max_bytes=100) == b"VALUE = 1\n"
    git("switch", "fixture-edit")
    second = _native(backing, "import json,runpy; runpy.run_path('/data/run_tests.py'); print(json.dumps({'binary':list(open('/data/binary.bin','rb').read()),'value':runpy.run_path('/data/current.py')['VALUE']}))")
    assert second == {"binary": list(range(256)), "value": 2}
    assert git("status", "--porcelain") == ""
    git("fsck", "--strict")


def test_sqlite_transactions_and_committed_rows_survive_real_disk_exhaustion(backing):
    result = _native(
        backing,
        """import sqlite3,json
db=sqlite3.connect('/data/full.sqlite')
db.execute('PRAGMA journal_mode=DELETE')
db.execute('CREATE TABLE entries (id INTEGER PRIMARY KEY, payload BLOB)')
db.execute('INSERT INTO entries VALUES (1,?)',(b'retained',)); db.commit()
db.execute('BEGIN IMMEDIATE')
peer=sqlite3.connect('/data/full.sqlite',timeout=0)
try:
    peer.execute('BEGIN IMMEDIATE')
    raise AssertionError('Second writer bypassed SQLite locking')
except sqlite3.OperationalError as exc:
    lock=exc.sqlite_errorcode
finally:
    peer.close(); db.rollback()
db.execute('INSERT INTO entries VALUES (2,?)',(b'rolled-back',)); db.rollback()
assert db.execute('SELECT COUNT(*) FROM entries').fetchone()==(1,)
committed=1; error=None
try:
    for identifier in range(2,202):
        db.execute('INSERT INTO entries VALUES (?,?)',(identifier,b'x'*(1<<20)))
        db.commit(); committed+=1
except sqlite3.OperationalError as exc:
    error=exc.sqlite_errorcode; db.rollback()
finally:
    db.close()
with sqlite3.connect('/data/full.sqlite') as reopened:
    integrity=reopened.execute('PRAGMA integrity_check').fetchone()[0]
    count=reopened.execute('SELECT COUNT(*) FROM entries').fetchone()[0]
    first=reopened.execute('SELECT payload FROM entries WHERE id=1').fetchone()[0].decode()
print(json.dumps({'error':error,'lock':lock,'integrity':integrity,'committed':committed,'rows':count,'first':first}))
""",
    )
    import sqlite3

    assert result["error"] == sqlite3.SQLITE_FULL and result["lock"] == sqlite3.SQLITE_BUSY
    assert result["integrity"] == "ok" and result["first"] == "retained"
    assert result["rows"] == result["committed"] > 1
    backing[0].verify_reserve()


def test_host_created_files_and_folders_are_usable_by_the_native_view(backing):
    with backing[1].filesystem() as fs:
        fs.mkdir("notes")
        fs.write_atomic("notes/page", b"host-created", expected_sha256=None, create=True)
    result = _native(backing, "import json; before=open('/data/notes/page','rb').read(); open('/data/notes/page','wb').write(b'native-edited'); print(json.dumps({'before':before.decode()}))")
    assert result == {"before": "host-created"}
    with backing[1].filesystem() as fs:
        assert fs.read_bytes("notes/page", max_bytes=100) == b"native-edited"


def test_arbitrary_native_byte_growth_hits_the_kernel_bound_and_keeps_reserve(backing):
    catalog, volume, _image, _containment = backing
    result = _native(
        backing,
        "import os,errno,json; fd=os.open('/data/full',os.O_CREAT|os.O_WRONLY,0o600); chunk=b'x'*(1<<20); error=None\n"
        "try:\n while True: os.write(fd,chunk)\nexcept OSError as exc: error=exc.errno\nfinally: os.close(fd)\n"
        "print(json.dumps({'errno':error,'size':os.stat('/data/full').st_size}))",
    )
    assert result["errno"] == errno.ENOSPC
    assert 0 < result["size"] <= volume.spec.max_bytes
    catalog.verify_reserve()
    assert catalog.verify(volume.spec.slot_id, previous=volume).available_bytes < 1 << 20


def test_arbitrary_native_inode_growth_hits_the_kernel_bound(backing):
    result = _native(
        backing,
        "import os,json; count=0; error=None\ntry:\n while True:\n"
        "  fd=os.open('/data/f-'+str(count),os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600); os.close(fd); count+=1\n"
        "except OSError as exc: error=exc.errno\nprint(json.dumps({'errno':error,'files':count}))",
    )
    assert result["errno"] == errno.ENOSPC
    assert 0 < result["files"] <= backing[1].spec.max_inodes
    backing[0].verify_reserve()


def test_full_filesystem_edit_leaves_the_existing_file_intact(backing):
    volume = backing[1]
    with volume.filesystem() as fs:
        fs.write_atomic("page", b"before", expected_sha256=None, create=True)
    result = _native(
        backing,
        "import os,json; fd=os.open('/data/full',os.O_CREAT|os.O_WRONLY,0o600); error=None\ntry:\n while True: os.write(fd,b'x'*(1<<20))\nexcept OSError as exc: error=exc.errno\nfinally: os.close(fd)\nprint(json.dumps({'errno':error}))",
    )
    assert result["errno"] == errno.ENOSPC
    # Native ENOSPC need not exhaust blocks available to a privileged host
    # writer (ext4 also reserves internal metadata clusters). The attempted
    # replacement exceeds this filesystem's total bound, rather than assuming
    # that a small host write must fail at the native writer's earlier limit.
    with volume.filesystem() as fs:
        from deerflow.spaces.filesystem import SpaceFull

        with pytest.raises(SpaceFull):
            fs.write_atomic("page", b"x" * volume.spec.max_bytes, expected_sha256=hashlib.sha256(b"before").hexdigest())
        assert fs.read_bytes("page", max_bytes=100) == b"before"
    assert not list(volume.control_path.glob("stage-*"))


def test_private_backup_growth_counts_against_the_same_bound(backing):
    volume = backing[1]
    with pytest.raises(OSError) as raised:
        with (volume.control_path / "retained-backup").open("wb", buffering=0) as backup:
            while True:
                backup.write(b"x" * (1 << 20))
    assert raised.value.errno == errno.ENOSPC
    assert backing[0].verify(volume.spec.slot_id, previous=volume).available_bytes < 1 << 20
    backing[0].verify_reserve()


def test_readonly_view_and_private_mount_boundary(backing):
    result = _native(
        backing,
        "import os,json; error=None\ntry: open('/data/forbidden','wb').write(b'x')\nexcept OSError as exc: error=exc.errno\n"
        "print(json.dumps({'errno':error,'control':os.path.exists('/control'),'socket':os.path.exists('/var/run/docker.sock')}))",
        readonly=True,
    )
    assert result == {"errno": errno.EROFS, "control": False, "socket": False}


def test_reopening_inventory_preserves_identity_and_root_replacement_is_detected(backing):
    catalog, volume, _image, _containment = backing
    restarted = PreparedVolumeCatalog.from_manifest(catalog.data_disk / "inventory.v1.json")
    assert restarted.verify(volume.spec.slot_id, previous=volume).spec == volume.spec
    retired = volume.data_path.with_name("retired-data")
    volume.data_path.rename(retired)
    volume.data_path.mkdir()
    try:
        with pytest.raises(BackingUnavailable, match="incarnation"):
            catalog.verify(volume.spec.slot_id, previous=volume)
    finally:
        volume.data_path.rmdir()
        retired.rename(volume.data_path)


def test_mounted_uuid_mismatch_is_refused_before_file_capability(backing, monkeypatch):
    import deerflow.spaces.backings as module

    monkeypatch.setattr(module, "mounted_ext4_identity", lambda fd: str(uuid.uuid4()))
    with pytest.raises(BackingUnavailable, match="Mounted filesystem UUID"):
        backing[0].verify(backing[1].spec.slot_id)


@pytest.mark.asyncio
async def test_resource_http_and_durable_binding_use_the_qualified_volume(backing):
    from types import SimpleNamespace

    import httpx
    from fastapi import FastAPI
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.gateway.routers.spaces import router
    from deerflow.persistence.base import Base
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceBackupRow, SpaceMountRow
    from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
    from deerflow.spaces.contract import Custody, MutationMode, PrincipalRef, ResolvedPrincipal
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    catalog, _volume, _image, _containment = backing
    engine = create_async_engine(f"sqlite+aiosqlite:///{catalog.data_disk / 'http-fixture.db'}")
    actor = PrincipalRef("human", "qualified-http-fixture")

    async def lookup(reference):
        return ResolvedPrincipal(actor) if reference == actor else None

    try:
        async with engine.begin() as c:
            await c.run_sync(Base.metadata.create_all, tables=[model.__table__ for model in (SpaceRow, SpaceGrantRow, SpaceEventRow, SpaceBackingRow, SpaceFileOperationRow, SpaceAttachmentRow, SpaceMountRow, SpaceBackupRow)])
        sf = async_sessionmaker(engine, expire_on_commit=False)
        registry = SpaceRegistry(sf, HostPrincipalResolver(human=lookup))
        service = SpaceFiles(registry, catalog)
        space = await service.create(actor=actor, name="Qualified resource", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
        app = FastAPI()
        app.state.storage_spaces = service

        @app.middleware("http")
        async def authenticated_fixture(request, call_next):
            request.state.user = SimpleNamespace(id=actor.subject_id, system_role="user")
            request.state.auth_source = "session"
            return await call_next(request)

        app.include_router(router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://qualified-fixture") as client:
            endpoint = f"/api/spaces/{space.id}"
            response = await client.put(endpoint + "/content", params={"path": ".gitignore", "generation": 1, "operation_id": uuid.uuid4().hex, "create": "true"}, content=b"*.tmp\n")
            assert response.status_code == 200, response.text
            app.state.storage_spaces = SpaceFiles(registry, PreparedVolumeCatalog.from_manifest(catalog.data_disk / "inventory.v1.json"))
            response = await client.get(endpoint + "/content", params={"path": ".gitignore"}, headers={"Range": "bytes=0-4"})
            assert response.status_code == 206 and response.content == b"*.tmp"
            quota = (await client.get(endpoint)).json()["quota"]
            assert quota["backend"] == "fixed-ext4"
            assert quota["max_bytes"] == backing[1].spec.max_bytes
            assert 0 < quota["available_bytes"] < quota["max_bytes"]
            assert quota["max_inodes"] == backing[1].spec.max_inodes
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("exhaustion", ["bytes", "inodes", "held-bytes"])
async def test_host_reclaims_full_private_control_and_renews_roots(backing, tmp_path, exhaustion):
    """Real ENOSPC, root renewal and fresh native writes; mandatory CI only."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.persistence.base import Base
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
    from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceBackupRow, SpaceMountRow
    from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
    from deerflow.spaces.contract import Custody, MutationMode, PrincipalRef, ResolvedPrincipal
    from deerflow.spaces.incarnations import BoundVolume
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.recovery import SpaceRecovery
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles, SpaceOperationPending

    catalog, volume, _image, _containment = backing
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'release.db'}")
    sf = async_sessionmaker(engine, expire_on_commit=False)
    actor = PrincipalRef("human", "native-release")
    held = None

    async def lookup(reference):
        return ResolvedPrincipal(reference) if reference == actor else None

    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all, tables=[model.__table__ for model in (SpaceRow, SpaceGrantRow, SpaceEventRow, SpaceBackingRow, SpaceFileOperationRow, SpaceAttachmentRow, SpaceMountRow, SpaceBackupRow)])
        files = SpaceFiles(SpaceRegistry(sf, HostPrincipalResolver(human=lookup)), catalog)
        space = await files.create(actor=actor, name="Retiring", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
        before = catalog.verify(volume.spec.slot_id)
        stale = BoundVolume(before, space.backing_handle)
        owned = stale.filesystem()
        try:
            assert not any(before.data_path.iterdir())
            if exhaustion == "inodes":
                for index in range(before.spec.max_inodes + 1):
                    try:
                        (before.control_path / f"full-{index}").touch(exist_ok=False)
                    except OSError as exc:
                        assert exc.errno == errno.ENOSPC
                        break
                else:
                    pytest.fail("Prepared filesystem did not enforce its inode bound")
                assert catalog.verify(before.spec.slot_id).available_inodes == 0
            else:
                held = os.open(before.control_path / "full-private-backup", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                # Large ext4 writes can fail while smaller allocations still fit.
                # Exhaust one filesystem block at a time to deny journal creation.
                chunk = bytes(os.statvfs(before.control_path).f_frsize)
                for _ in range(before.spec.max_bytes // len(chunk) + 2):
                    try:
                        os.write(held, chunk)
                    except OSError as exc:
                        assert exc.errno == errno.ENOSPC
                        break
                else:
                    pytest.fail("Prepared filesystem did not enforce its byte bound")
                if exhaustion != "held-bytes":
                    os.close(held)
                    held = None
                assert catalog.verify(before.spec.slot_id).available_bytes < 4096
            async with sf() as session, session.begin():
                row = await session.get(SpaceRow, space.id)
                row.status, row.generation = "deleted", 2
                session.add(SpaceEventRow(space_id=space.id, generation=2, action="delete", actor_kind="human", actor_id=actor.subject_id, details={"retained_backups": False, "retained_binding": True, "cause": "seat-released"}))
            if exhaustion == "held-bytes":
                with pytest.raises(SpaceOperationPending):
                    await SpaceRecovery(files).host_release(space.id)
                async with sf() as session:
                    assert await session.get(SpaceBackingRow, space.backing_handle) is not None
                assert os.fstat(held).st_nlink == 0
                os.close(held)
                held = None
            result = await SpaceRecovery(files).host_release(space.id)
            assert result["newly_reclaimed"] and result["slot_released"]
            renewed = catalog.verify(before.spec.slot_id)
            assert renewed.spec.filesystem_uuid == before.spec.filesystem_uuid
            assert renewed.root_inode != before.root_inode and renewed.control_inode != before.control_inode
            assert not any(renewed.data_path.iterdir()) and not any(renewed.control_path.iterdir())
            assert renewed.available_bytes > 1 << 20 and renewed.available_inodes > 16
            with pytest.raises(OSError):
                os.mkdir("late-write", dir_fd=owned._data_fd)
            with pytest.raises(BackingUnavailable):
                stale.filesystem()
            replacement = await files.create(actor=actor, name="Replacement", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
            assert replacement.backing_handle != space.backing_handle
            outcome = _native(backing, "import pathlib,json; p=pathlib.Path('/data/new-resource'); p.write_text('fresh native view'); print(json.dumps({'value':p.read_text()}))")
            assert outcome == {"value": "fresh native view"}
        finally:
            owned.close()
    finally:
        if held is not None:
            os.close(held)
        await engine.dispose()


def test_gateway_uid_1000_owns_stable_operator_lock(typed_prepared_pool):
    """Actual unprivileged Gateway identity can lock, but cannot replace, it."""
    import stat
    import traceback

    from deerflow.spaces.docker import StorageHostUnqualified
    from deerflow.spaces.runtime_owner import RUNTIME_LOCK, PoolRuntimeOwner

    pool = typed_prepared_pool.pool
    metadata = (pool / RUNTIME_LOCK).stat()
    assert (metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode), metadata.st_nlink, metadata.st_size) == (1000, 1000, 0o600, 1, 0)
    child = os.fork()
    if child == 0:
        first, second = PoolRuntimeOwner(pool), PoolRuntimeOwner(pool)
        try:
            os.setgroups([])
            os.setgid(1000)
            os.setuid(1000)
            first.acquire()
            try:
                second.acquire()
            except StorageHostUnqualified:
                pass
            else:
                os._exit(2)
            try:
                (pool / RUNTIME_LOCK).unlink()
            except PermissionError:
                pass
            else:
                os._exit(3)
            first.close()
            second.acquire()
            second.close()
        except BaseException:
            os.write(2, traceback.format_exc().encode("utf-8"))
            os._exit(1)
        os._exit(0)
    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert (pool / RUNTIME_LOCK).stat().st_ino == metadata.st_ino


def test_gateway_uid_1000_can_persist_and_reopen_native_create_evidence(typed_prepared_pool):
    """Real operator ownership supports durable private Gateway journal IO."""
    import stat
    import traceback

    from deerflow.spaces.native_create_evidence import DIRECTORY, NativeContainerIdentity, NativeCreateEvidence
    from deerflow.spaces.runtime_owner import PoolRuntimeOwner

    pool = typed_prepared_pool.pool
    metadata = (pool / DIRECTORY).stat()
    assert (metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode)) == (1000, 1000, 0o700)
    slot = next(spec.slot_id for spec in typed_prepared_pool.load().volumes.values() if spec.kind == "user")
    child = os.fork()
    if child == 0:
        owner, evidence = PoolRuntimeOwner(pool), None
        try:
            os.setgroups([])
            os.setgid(1000)
            os.setuid(1000)
            owner.acquire()
            evidence = NativeCreateEvidence(pool, daemon_id="native-journal-fixture")
            attempt = evidence.begin(owner_id="journal-fixture", thread_id="chat", name="hartmesh-journal-fixture", slots=(slot,))
            evidence.close()
            evidence = NativeCreateEvidence(pool, daemon_id="native-journal-fixture")
            assert evidence.pending(slots=(slot,)) == (attempt,)
            record = pool / DIRECTORY / (attempt.id + ".json")
            facts = record.stat()
            assert (facts.st_uid, facts.st_gid, stat.S_IMODE(facts.st_mode), facts.st_nlink) == (1000, 1000, 0o600, 1)
            # This case proves journal permissions/durability, not Docker
            # identity. The bridge must supply actual inspected identity.
            verified = NativeContainerIdentity(attempt.id, attempt.daemon_id, attempt.owner_id, attempt.thread_id, attempt.name, attempt.slots, "c" * 64)
            evidence.acknowledge(attempt, verified)
            assert evidence.pending() == ()
            evidence.close()
            owner.close()
            try:
                (pool / DIRECTORY).rmdir()
            except PermissionError:
                pass
            else:
                os._exit(2)
        except BaseException:
            os.write(2, traceback.format_exc().encode("utf-8"))
            os._exit(1)
        os._exit(0)
    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert (pool / DIRECTORY).stat().st_ino == metadata.st_ino


def test_gateway_uid_1000_submits_and_acknowledges_actual_managed_primary(typed_prepared_pool, monkeypatch):
    """CI-only real builder, daemon inspection, private journal and UID-1000 IO."""
    import logging
    import traceback
    from threading import Lock

    from test_aio_sandbox_provider import _make_provider

    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
    from deerflow.sandbox.lease import discard_sandbox_lease_manager
    from deerflow.spaces.docker import DockerStorageAdapter
    from deerflow.spaces.native_create_evidence import ATTEMPT_LABEL
    from deerflow.spaces.runtime_owner import PoolRuntimeOwner

    tool = typed_prepared_pool
    image = os.environ.get("HARTMESH_INSTANCE_AIO_IMAGE")
    assert image and "@sha256:" in image
    if runtime := os.environ.get("HARTMESH_NATIVE_WRITER_RUNTIME"):
        monkeypatch.setenv("DEER_FLOW_SANDBOX_RUNTIME", runtime)
    for key, value in {"DEER_FLOW_SANDBOX_IMAGE_STARTUP_CAPS": "0", "DEER_FLOW_SANDBOX_MEMORY": "768m", "DEER_FLOW_SANDBOX_CPUS": "1", "DEER_FLOW_SANDBOX_PIDS_LIMIT": "128", "DEER_FLOW_SANDBOX_CONTAINER_USER": "1000:1000"}.items():
        monkeypatch.setenv(key, value)
    config = tool.pool.parent / ("docker-config-" + uuid.uuid4().hex)
    config.mkdir(mode=0o700)
    os.chown(config, 1000, 1000)
    monkeypatch.setenv("DOCKER_CONFIG", str(config))
    socket_group = Path("/var/run/docker.sock").stat().st_gid
    spec = next(s for s in tool.load().volumes.values() if s.kind == "user")
    sandbox = uuid.uuid4().hex
    name = "hartmesh-bridge-native-" + sandbox
    tool._native_bridge_contained = False
    child = os.fork()
    if child == 0:
        # pytest's in-memory capture does not return logs from a forked
        # child. Keep containment diagnostics on its captured stderr.
        native_logger = logging.getLogger("deerflow.community.aio_sandbox")
        native_logger.addHandler(logging.StreamHandler())
        native_logger.setLevel(logging.INFO)
        owner = PoolRuntimeOwner(tool.pool)
        handoff = adapter = backend = provider = None
        exit_code = 0
        try:
            os.setgroups([socket_group])
            os.setgid(1000)
            os.setuid(1000)
            provider = _make_provider(tool.pool.parent)
            provider._lock = Lock()
            provider._managed_runtime_required = True
            backend = provider._backend = LocalContainerBackend(
                image=image,
                base_port=0,
                container_prefix="hartmesh-bridge-native",
                config_mounts=[],
                environment={key: "true" for key in ("DISABLE_BROWSER", "DISABLE_JUPYTER", "DISABLE_CODE_SERVER", "DISABLE_VNC", "DISABLE_MCP_BROWSER", "DISABLE_NODEJS_REPL")},
            )
            owner.resolve_provider(lambda: provider)
            backend._runtime_runner = owner.run
            adapter = DockerStorageAdapter.from_local_backend(backend, pool=tool.pool, clean_probe=False)
            evidence = owner.qualify_native_evidence(provider, daemon_id=adapter.host_id)
            provider._install_native_creation(evidence, adapter)
            assert provider._pool_runtime_current()
            bridge = provider._native_creation
            labels = {**backend._sandbox_labels(sandbox), **backend._owner_labels("native-bridge-fixture", "chat"), "hartmesh.chat.storage": "on", "hartmesh.storage.space." + spec.slot_id: "1"}
            container_id = backend._start_container(name, 0, [(str(spec.mount_path / "data"), "/mnt/user-data", False)], publish_port=False, labels=labels)
            with provider._lock:
                (handoff,) = tuple(bridge.handoffs._items.values())
                assert handoff.acknowledged and handoff.verified.container_id == container_id
                assert bridge.handoffs.extra_slots_locked(starting={}, known_container_ids=set()) == 1
            assert evidence.pending() == ()
            actual = adapter._inspect(container_id)
            assert actual["Config"]["Labels"][ATTEMPT_LABEL] == handoff.attempt.id
            assert actual["Config"]["User"] == "1000:1000"
            assert any(mount["Source"] == str(spec.mount_path / "data") and mount["Destination"] == "/mnt/user-data" and mount["RW"] for mount in actual["Mounts"])
            result = backend._run(
                ["docker", "exec", "--user", "1000:1000", container_id, "python3", "-c", "import os; assert os.getuid()==os.getgid()==1000; open('/mnt/user-data/native-bridge-proof','wb').write(b'actual managed primary')"],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert result.returncode == 0
            # Publish actual tracking through the ordinary accounting transfer,
            # then exercise the production retained scope and canonical cleanup.
            info = SandboxInfo(sandbox, "", container_name=name, container_id=container_id, owner=("native-bridge-fixture", "chat"), chat_storage_enabled=True, native_slots=(spec.slot_id,))
            with provider._lock:
                provider._warm_pool[sandbox] = (info, 0)
                provider._warm_pool_identity[sandbox] = info.owner
                bridge.handoffs.transferred_locked(handoff)

            async def contain():
                async with provider._native_retirement_scope(info.owner[0], spec.slot_id) as scope:
                    await scope.prove_absent()
                    assert not provider._warm_pool and not provider._sandboxes
                assert await asyncio.to_thread(backend._slot_containers, spec.slot_id) == ()

            asyncio.run(contain())
            assert adapter._inspect(container_id) is None
            # Exercise a real stopped, non-auto-remove primary too. The actual
            # builder and durable request stay intact; this CI-only runner
            # omits auto-removal from Docker create so canonical rm is required.
            retained_sid = uuid.uuid4().hex
            retained_name = "hartmesh-bridge-stopped-" + retained_sid

            def retained_runner(command, **kwargs):
                if command[:2] == ["docker", "create"]:
                    command = [value for value in command if value != "--rm"]
                return owner.run(command, **kwargs)

            backend._runtime_runner = retained_runner
            retained_labels = {**labels, "deerflow.sandbox_id": retained_sid}
            retained_id = backend._start_container(retained_name, 0, [(str(spec.mount_path / "data"), "/mnt/user-data", False)], publish_port=False, labels=retained_labels, start=False)
            retained = adapter._inspect(retained_id)
            assert retained["State"]["Running"] is False and retained["HostConfig"]["AutoRemove"] is False
            info = SandboxInfo(retained_sid, "", container_name=retained_name, container_id=retained_id, owner=("native-bridge-fixture", "chat"), chat_storage_enabled=True, native_slots=(spec.slot_id,))
            with provider._lock:
                (handoff,) = tuple(bridge.handoffs._items.values())
                assert handoff.acknowledged and handoff.verified.container_id == retained_id
                provider._warm_pool[retained_sid] = (info, 0)
                provider._warm_pool_identity[retained_sid] = info.owner
                bridge.handoffs.transferred_locked(handoff)
            asyncio.run(contain())
            assert adapter._inspect(retained_id) is None
            with provider._lock:
                assert bridge.handoffs.extra_slots_locked(starting={}, known_container_ids=set()) == 0
            assert evidence.pending() == ()
        except BaseException:
            os.write(2, traceback.format_exc().encode("utf-8"))
            exit_code = 1
        finally:
            if handoff is not None and handoff.verified is not None and adapter is not None:
                try:
                    if adapter._inspect(handoff.verified.container_id) is not None:
                        backend._run(["docker", "rm", "-f", handoff.verified.container_id], check=True, capture_output=True, text=True, timeout=30)
                    assert adapter._inspect(handoff.verified.container_id) is None
                except BaseException:
                    os.write(2, traceback.format_exc().encode("utf-8"))
                    exit_code = 2
            if provider is not None:
                discard_sandbox_lease_manager(provider)
                provider._acquire_serializer.close()
                provider._acquire_worker_executor.shutdown(wait=True)
                provider._ownership.close()
            owner.close()
        os._exit(exit_code)
    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    tool._native_bridge_contained = True
    assert (spec.mount_path / "data/native-bridge-proof").read_bytes() == b"actual managed primary"
    (spec.mount_path / "data/native-bridge-proof").unlink()
    shutil.rmtree(config)

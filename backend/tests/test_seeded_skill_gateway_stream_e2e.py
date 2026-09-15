"""A seeded public skill on the non-durable profile: a normal turn still runs.

Regression for the first tenant release that carried a public skill library
(v2.1.0+hartmesh.13): every Gateway run is an accepted invocation, a nonempty
effective-skill snapshot makes its materialization mandatory before the run
starts, and the worker refused a provider without a qualified durable
materializer whenever a run record existed, which is always. The first
browser turn on the tenant VM ended in ``AcceptedSkillSandboxBindingError``
before any sandbox existed. Under ``local_development`` the accepted-skills
projection is the path (``docs/ACCEPTED_SANDBOX_EXECUTION.md``, "Which
population a deployment profile runs").

What runs for real: the same Gateway, route, worker, admission and journal as
``test_turn_phase_gateway_stream_e2e.py``, plus one public skill seeded into
the library the Gateway snapshots at admission, and the local sandbox
provider's accepted-skills projection binding that snapshot before the model
is called. The probe model emits no tool call, so the sandbox is provisioned
and bound but never commanded; the projection's contents are pinned by the
provider suites. The host-local provider answers ``empty_only`` for accepted
material, so the sandbox is ``_seeded_skill_sandbox_provider.ProjectionProvider``,
which declares the immutability the tenant's container backend declares.
"""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e

_SEEDED_SKILL = """\
---
name: tenant-note
description: Write a short note; seeded to prove a normal turn survives a nonempty skill library.
---

Write the note the person asked for, in plain words.
"""


@pytest.fixture(scope="module")
def seeded_gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    home = tmp_path_factory.mktemp("seeded-skill-e2e")
    skill_dir = home / "skills" / "public" / "tenant-note"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(_SEEDED_SKILL, encoding="utf-8")
    config_yaml = e2e._MINIMAL_CONFIG_YAML.replace(
        "use: deerflow.sandbox.local:LocalSandboxProvider",
        "use: _seeded_skill_sandbox_provider:ProjectionProvider",
    )
    assert config_yaml != e2e._MINIMAL_CONFIG_YAML
    with e2e.serve_gateway(home, config_yaml=config_yaml) as served:
        yield served


def test_a_seeded_public_skill_does_not_break_a_normal_turn(seeded_gateway: e2e._Gateway) -> None:
    base = seeded_gateway.loopback_url
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        listed = client.get(f"{base}/api/skills")
        assert listed.status_code == 200, listed.text
        names = {entry["name"]: entry for entry in listed.json().get("skills", [])}
        assert names["tenant-note"]["enabled"] is True, "the seeded skill must be enabled, or the snapshot is empty and this proves nothing"
        first = e2e._observe_stream(client, base, thread_id, csrf, "probe:text please")
        second = e2e._observe_stream(client, base, thread_id, csrf, "probe:text again")

    for observed in (first, second):
        assert observed.events[-1] == "end", observed.events
        assert "error" not in observed.events, observed.events
        assert observed.text_frames >= 1
        wire = seeded_gateway.journals.wait_for(observed.run_id)
        assert wire["outcome"] == "success", wire
        assert wire["session_kind"] == "accepted", wire
        assert wire["snapshot_present"] is True and wire["snapshot_package_count"] == 1, wire
        assert wire["mandatory_materialization"] is True, wire
        assert e2e._phase_at(wire, "model_request") is not None, "the model must have been reached after materialization"

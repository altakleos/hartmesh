from types import SimpleNamespace

import pytest

from deerflow.config.paths import Paths, paths_scope
from deerflow.features.paths import FeaturePaths
from deerflow.features.publications import PublicationUnavailable, reconcile_publication
from deerflow.files.shared import shared_mutation_state


@pytest.mark.asyncio
async def test_removal_journal_cannot_settle_another_publication_under_the_same_filename(tmp_path):
    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    (data / "alpha.txt").write_bytes(b"retain alpha")
    paths = FeaturePaths(Paths(tmp_path / "unused"), "alice", {"hm.shared": SimpleNamespace(data_path=data, control_path=control)})
    with paths_scope(paths):
        state = shared_mutation_state()
        state.acquire()
        pending = state.stage("alpha.txt", "alpha-publication")
        journal = pending.name
        payload = control / journal / "payload"
        pending.journal.update(path="beta.txt", publication_id="removed-beta-publication")
        pending.write_journal()
        pending.close()
        state.close()

        class Repo:
            async def publication(self, identifier):
                assert identifier == "removed-beta-publication"
                return {"path": "beta.txt", "removed_at": "already-removed"}

        receipt = SimpleNamespace(value={"facts": {"removal": {"journal": journal, "path": "alpha.txt", "publication_id": "alpha-publication"}}})
        intent = SimpleNamespace(request={"operation": "remove_shared", "parameters": {"expected_publication_id": "alpha-publication"}})
        with pytest.raises(PublicationUnavailable, match="exact operation receipt"):
            await reconcile_publication(Repo(), receipt, intent)
    assert payload.read_bytes() == b"retain alpha"
    assert not (data / "beta.txt").exists()

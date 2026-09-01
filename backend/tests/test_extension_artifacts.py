from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from deerflow.extensions.artifacts import (
    ExtensionArtifactManifestV1,
    ExtensionArtifactVerificationError,
    ExtensionSourceLockEntryV1,
    ExtensionSourceLockV1,
    build_installed_artifact_manifest,
    build_source_lock,
    canonical_platform_tag,
    extension_configuration_digest,
    extension_configuration_projection,
    hash_local_snapshot_tree,
    read_source_lock,
    verify_installed_artifact_manifest,
    verify_source_lock_current,
)

_BACKEND_ROOT = Path(__file__).resolve().parents[1]


def test_empty_source_lock_has_one_canonical_digest_and_json() -> None:
    lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(),
    )

    assert lock.digest == "sha256:36e11560586b9253041cc0abadb0f721d378e6845f060f876d7fc7c9a868726c"
    assert lock.to_dict() == {
        "version": 1,
        "extension_api_version": "0.13.0",
        "entries": [],
        "digest": "sha256:36e11560586b9253041cc0abadb0f721d378e6845f060f876d7fc7c9a868726c",
    }
    assert ExtensionSourceLockV1.from_dict(json.loads(lock.to_json())) == lock


def test_verification_diagnostics_are_bounded_and_correlated() -> None:
    error = ExtensionArtifactVerificationError(
        "extension_artifact_digest_mismatch",
        distribution="Acme_Ext",
        expected_digest="sha256:" + ("a" * 64),
        actual_digest="sha256:" + ("b" * 64),
    )

    rendered = str(error)
    assert "distribution=acme-ext" in rendered
    assert "expected=sha256:" + ("a" * 12) in rendered
    assert "actual=sha256:" + ("b" * 12) in rendered
    assert f"correlation_id={error.correlation_id}" in rendered
    assert len(error.correlation_id) == 32
    assert ("a" * 64) not in rendered


def test_source_lock_parser_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    source_lock = tmp_path / "extensions.lock.json"
    source_lock.write_text(
        '{"version":1,"version":1,"extension_api_version":"0.13.0","entries":[],"digest":"sha256:' + ("0" * 64) + '"}\n',
        encoding="utf-8",
    )

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_manifest_invalid",
    ):
        read_source_lock(source_lock)


def test_committed_source_lock_matches_current_dependency_lock() -> None:
    source_lock = read_source_lock(_BACKEND_ROOT / "extensions.lock.json")

    assert verify_source_lock_current(source_lock, _BACKEND_ROOT) is source_lock


def test_stale_source_lock_is_rejected_without_rewriting_it(tmp_path: Path) -> None:
    backend = tmp_path / "backend"
    backend.mkdir()
    (backend / "pyproject.toml").write_text(
        '[dependency-groups]\nextensions = ["acme-ext==1.2.3"]\n',
        encoding="utf-8",
    )
    (backend / "uv.lock").write_text(
        """\
version = 1

[[package]]
name = "acme-ext"
version = "1.2.3"
source = { registry = "https://pypi.org/simple" }
wheels = [{ url = "https://files.example/acme.whl", hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }]
""",
        encoding="utf-8",
    )
    stale = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(),
    )
    before = stale.to_json()

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_digest_mismatch",
    ):
        verify_source_lock_current(stale, backend)

    assert stale.to_json() == before


def test_registry_source_entry_binds_exact_identity_and_locked_hashes() -> None:
    entry = ExtensionSourceLockEntryV1.create(
        distribution="Acme_Ext",
        distribution_version="1.2.3",
        entry_point_name="policy",
        entry_point_value="acme_ext:install",
        source_kind="registry",
        source_reference="https://pypi.org/simple",
        source_revision=None,
        locked_artifact_hashes=("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",),
        local_tree_digest=None,
    )
    lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(entry,),
    )

    assert entry.entry_digest == "sha256:db05f973653d07f113e798a118f440804c198134ccc2570975787825faaa919d"
    assert lock.digest == "sha256:0b077b40232312a2b43771804c24b84d13967e07035b5a88a45158c1a1161395"
    assert ExtensionSourceLockV1.from_dict(lock.to_dict()) == lock


def test_loopback_git_source_identity_preserves_its_http_scheme() -> None:
    entry = ExtensionSourceLockEntryV1.create(
        distribution="acme-ext",
        distribution_version="1.2.3",
        entry_point_name="policy",
        entry_point_value="acme_ext:install",
        source_kind="git",
        source_reference="http://127.0.0.1:9418/acme/ext.git",
        source_revision="b" * 40,
        locked_artifact_hashes=(),
        local_tree_digest=None,
    )

    assert entry.source_reference == "http://127.0.0.1:9418/acme/ext.git"


def test_local_snapshot_tree_digest_is_order_stable_and_uses_manager_exclusions(
    tmp_path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    for root, order in ((first, ("b.txt", "a.py")), (second, ("a.py", "b.txt"))):
        (root / "package").mkdir(parents=True)
        for name in order:
            (root / "package" / name).write_text(name, encoding="utf-8")
        (root / ".git").mkdir()
        (root / ".git" / "config").write_text("credential=do-not-read", encoding="utf-8")
        (root / "package" / "__pycache__").mkdir()
        (root / "package" / "__pycache__" / "a.pyc").write_bytes(b"generated")

    original = hash_local_snapshot_tree(first)
    assert hash_local_snapshot_tree(second) == original

    (second / "package" / "a.py").write_text("a.px", encoding="utf-8")
    assert hash_local_snapshot_tree(second) != original


def test_registry_source_lock_extracts_exact_uv_version_and_all_archive_hashes(
    tmp_path,
) -> None:
    backend = tmp_path / "backend"
    backend.mkdir()
    (backend / "pyproject.toml").write_text(
        '[dependency-groups]\nextensions = ["acme-ext==1.2.3"]\n',
        encoding="utf-8",
    )
    (backend / "uv.lock").write_text(
        """
version = 1

[[package]]
name = "acme-ext"
version = "1.2.3"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://files.example/acme-ext.tar.gz", hash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" }
wheels = [
  { url = "https://files.example/acme_ext.whl", hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
]
""".lstrip(),
        encoding="utf-8",
    )

    lock = build_source_lock(
        backend,
        plugins=(
            {
                "name": "policy",
                "package": "acme-ext",
                "use": "acme_ext:install",
            },
        ),
        extension_api_version="0.13.0",
    )

    assert len(lock.entries) == 1
    entry = lock.entries[0]
    assert entry.distribution_version == "1.2.3"
    assert entry.source_kind == "registry"
    assert entry.source_reference == "https://pypi.org/simple"
    assert entry.locked_artifact_hashes == (
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )


def test_git_source_lock_requires_and_records_one_full_commit(tmp_path) -> None:
    backend = tmp_path / "backend"
    backend.mkdir()
    (backend / "pyproject.toml").write_text(
        '[dependency-groups]\nextensions = ["acme-ext @ git+https://github.com/acme/ext.git"]\n',
        encoding="utf-8",
    )
    commit = "b" * 40
    lock_path = backend / "uv.lock"
    lock_path.write_text(
        f"""\
version = 1

[[package]]
name = "acme-ext"
version = "1.2.3"
source = {{ git = "https://github.com/acme/ext.git", rev = "{commit}", precise = "{commit}" }}
""",
        encoding="utf-8",
    )
    plugins = (
        {
            "name": "policy",
            "package": "acme-ext",
            "use": "acme_ext:install",
        },
    )

    locked = build_source_lock(
        backend,
        plugins=plugins,
        extension_api_version="0.13.0",
    )

    assert locked.entries[0].source_kind == "git"
    assert locked.entries[0].source_reference == "https://github.com/acme/ext.git"
    assert locked.entries[0].source_revision == commit

    lock_path.write_text(
        """\
version = 1

[[package]]
name = "acme-ext"
version = "1.2.3"
source = { git = "https://github.com/acme/ext.git", rev = "main" }
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="extension_source_not_immutable"):
        build_source_lock(
            backend,
            plugins=plugins,
            extension_api_version="0.13.0",
        )


def test_empty_installed_manifest_is_canonical() -> None:
    lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(),
    )

    manifest = build_installed_artifact_manifest(
        lock,
        platform_tag="cp312-cp312-manylinux_2_36_x86_64",
    )

    assert manifest.entries == ()
    assert ExtensionArtifactManifestV1.from_dict(manifest.to_dict()) == manifest
    assert manifest.digest.startswith("sha256:")

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_digest_mismatch",
    ):
        verify_installed_artifact_manifest(
            lock,
            manifest,
            expected_extension_api_version="0.14.0",
        )


def test_configuration_projection_replaces_secrets_with_field_path_handles() -> None:
    first = [
        {
            "name": "policy",
            "package": "acme-ext",
            "use": "acme_ext:install",
            "enabled": True,
            "required": True,
            "config": {"endpoint": "https://audit.example", "apiKey": "first"},
        }
    ]
    rotated = [
        {
            **first[0],
            "config": {"endpoint": "https://audit.example", "apiKey": "second"},
        }
    ]

    projection = extension_configuration_projection(first)

    assert "first" not in repr(projection)
    assert extension_configuration_digest(first) == extension_configuration_digest(rotated)
    rotated[0]["config"]["endpoint"] = "https://other.example"
    assert extension_configuration_digest(first) != extension_configuration_digest(rotated)


@pytest.mark.parametrize(
    "secret_key",
    ["myapikeysetting", "prefixsecretkeysuffix", "customercredentialblob"],
)
def test_configuration_projection_conservatively_handles_glued_secret_names(
    secret_key: str,
) -> None:
    first = [{"use": "acme_ext:install", "config": {secret_key: "first-secret"}}]
    rotated = [{"use": "acme_ext:install", "config": {secret_key: "rotated-secret"}}]

    projection = extension_configuration_projection(first)

    assert "first-secret" not in repr(projection)
    assert extension_configuration_digest(first) == extension_configuration_digest(rotated)


def test_configuration_projection_keeps_non_secret_author_field() -> None:
    first = [{"use": "acme_ext:install", "config": {"author": "Ada"}}]
    changed = [{"use": "acme_ext:install", "config": {"author": "Grace"}}]

    projection = extension_configuration_projection(first)

    assert projection["plugins"][0]["config"]["author"] == "Ada"
    assert extension_configuration_digest(first) != extension_configuration_digest(changed)


@pytest.mark.parametrize(
    "plugin",
    [
        {"use": "acme_ext:install", "enabled": 1},
        {"use": "acme_ext:install", "required": "yes"},
        {"use": "acme_ext:install", "package": object()},
        {"use": "acme_ext:install", "future_execution_field": True},
    ],
)
def test_configuration_projection_rejects_unclassified_or_wrongly_typed_fields(
    plugin: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        extension_configuration_projection([plugin])


class _Record(str):
    def __new__(cls, value: str, content: bytes):
        instance = super().__new__(cls, value)
        encoded = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).decode().rstrip("=")
        instance.hash = SimpleNamespace(mode="sha256", value=encoded)
        instance.size = len(content)
        return instance


class _Distribution:
    def __init__(
        self,
        root: Path,
        files: list[object],
        *,
        direct_url: str | None = None,
    ) -> None:
        self._root = root
        self.files = files
        self.direct_url = direct_url
        self.metadata = {"Name": "acme-ext"}
        self.version = "1.2.3"
        self.entry_points = [
            SimpleNamespace(
                group="deerflow.extensions",
                name="policy",
                value="acme_ext:install",
            )
        ]

    def locate_file(self, path) -> Path:
        return self._root / str(path)

    def read_text(self, name: str) -> str | None:
        return self.direct_url if name == "direct_url.json" else None


def _source_lock_for_installed_test() -> ExtensionSourceLockV1:
    return ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(
            ExtensionSourceLockEntryV1.create(
                distribution="acme-ext",
                distribution_version="1.2.3",
                entry_point_name="policy",
                entry_point_value="acme_ext:install",
                source_kind="registry",
                source_reference="https://pypi.org/simple",
                source_revision=None,
                locked_artifact_hashes=("sha256:" + ("a" * 64),),
                local_tree_digest=None,
            ),
        ),
    )


def test_installed_record_tamper_and_extra_owned_file_fail(tmp_path: Path) -> None:
    package = tmp_path / "acme_ext"
    package.mkdir()
    init = package / "__init__.py"
    init.write_bytes(b"def install(registry, config): pass\n")
    record = _Record("acme_ext/__init__.py", init.read_bytes())
    distribution = _Distribution(tmp_path, [record])

    build_installed_artifact_manifest(
        _source_lock_for_installed_test(),
        platform_tag="py3-none-any",
        find_distribution=lambda _name: distribution,
    )

    init.write_bytes(b"def install(registry, config): raise RuntimeError\n")
    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_installed_record_mismatch",
    ):
        build_installed_artifact_manifest(
            _source_lock_for_installed_test(),
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )

    init.write_bytes(b"def install(registry, config): pass\n")
    (package / "unrecorded.py").write_text("changed = True\n", encoding="utf-8")
    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_installed_record_mismatch",
    ):
        build_installed_artifact_manifest(
            _source_lock_for_installed_test(),
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )


def test_unhashed_owned_executable_is_rejected(tmp_path: Path) -> None:
    package = tmp_path / "acme_ext"
    package.mkdir()
    (package / "__init__.py").write_text(
        "def install(registry, config): pass\n",
        encoding="utf-8",
    )
    distribution = _Distribution(tmp_path, ["acme_ext/__init__.py"])

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_installed_record_mismatch",
    ):
        build_installed_artifact_manifest(
            _source_lock_for_installed_test(),
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )


def test_git_installation_requires_matching_pep610_commit_identity(
    tmp_path: Path,
) -> None:
    package = tmp_path / "acme_ext"
    package.mkdir()
    init = package / "__init__.py"
    init.write_text("def install(registry, config): pass\n", encoding="utf-8")
    commit = "b" * 40
    lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(
            ExtensionSourceLockEntryV1.create(
                distribution="acme-ext",
                distribution_version="1.2.3",
                entry_point_name="policy",
                entry_point_value="acme_ext:install",
                source_kind="git",
                source_reference="https://github.com/acme/ext.git",
                source_revision=commit,
                locked_artifact_hashes=(),
                local_tree_digest=None,
            ),
        ),
    )
    distribution = _Distribution(
        tmp_path,
        [_Record("acme_ext/__init__.py", init.read_bytes())],
        direct_url=json.dumps(
            {
                "url": "https://github.com/acme/ext.git",
                "vcs_info": {
                    "vcs": "git",
                    "commit_id": commit,
                    "requested_revision": commit,
                },
            }
        ),
    )

    build_installed_artifact_manifest(
        lock,
        platform_tag="py3-none-any",
        find_distribution=lambda _name: distribution,
    )

    distribution.direct_url = distribution.direct_url.replace(commit, "c" * 40)
    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_digest_mismatch",
    ):
        build_installed_artifact_manifest(
            lock,
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )

    distribution.direct_url = None
    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_digest_mismatch",
    ):
        build_installed_artifact_manifest(
            lock,
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )


def test_local_installation_requires_matching_pep610_snapshot_path(
    tmp_path: Path,
) -> None:
    backend = tmp_path / "backend"
    source = backend / "extensions" / "sources" / "acme-ext"
    source.mkdir(parents=True)
    (source / "pyproject.toml").write_text(
        '[project]\nname = "acme-ext"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )
    source_package = source / "acme_ext"
    source_package.mkdir()
    (source_package / "__init__.py").write_text("source = True\n", encoding="utf-8")
    lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(
            ExtensionSourceLockEntryV1.create(
                distribution="acme-ext",
                distribution_version="1.2.3",
                entry_point_name="policy",
                entry_point_value="acme_ext:install",
                source_kind="local_snapshot",
                source_reference="extensions/sources/acme-ext",
                source_revision=None,
                locked_artifact_hashes=(),
                local_tree_digest=hash_local_snapshot_tree(source),
            ),
        ),
    )
    installed = tmp_path / "installed"
    package = installed / "acme_ext"
    package.mkdir(parents=True)
    init = package / "__init__.py"
    init.write_text("def install(registry, config): pass\n", encoding="utf-8")
    distribution = _Distribution(
        installed,
        [_Record("acme_ext/__init__.py", init.read_bytes())],
        direct_url=json.dumps({"url": source.as_uri(), "dir_info": {}}),
    )

    build_installed_artifact_manifest(
        lock,
        backend_dir=backend,
        platform_tag="py3-none-any",
        find_distribution=lambda _name: distribution,
    )

    distribution.direct_url = json.dumps({"url": tmp_path.as_uri(), "dir_info": {}})
    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_digest_mismatch",
    ):
        build_installed_artifact_manifest(
            lock,
            backend_dir=backend,
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )


def test_startup_verification_rejects_manifest_from_another_platform() -> None:
    lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(),
    )
    current = canonical_platform_tag()
    other = "py3-none-any" if current != "py3-none-any" else "cp312-none-any"
    manifest = build_installed_artifact_manifest(lock, platform_tag=other)

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_digest_mismatch",
    ):
        verify_installed_artifact_manifest(lock, manifest)


def test_symlinked_installed_record_fails_before_file_hashing(tmp_path: Path) -> None:
    package = tmp_path / "acme_ext"
    package.mkdir()
    target = tmp_path / "outside.py"
    target.write_bytes(b"def install(registry, config): pass\n")
    init = package / "__init__.py"
    try:
        init.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable on this platform")
    distribution = _Distribution(
        tmp_path,
        [_Record("acme_ext/__init__.py", target.read_bytes())],
    )

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_installed_record_mismatch",
    ):
        build_installed_artifact_manifest(
            _source_lock_for_installed_test(),
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )


def test_installed_distribution_version_and_entry_point_are_exact(
    tmp_path: Path,
) -> None:
    package = tmp_path / "acme_ext"
    package.mkdir()
    init = package / "__init__.py"
    init.write_bytes(b"def install(registry, config): pass\n")
    distribution = _Distribution(
        tmp_path,
        [_Record("acme_ext/__init__.py", init.read_bytes())],
    )
    distribution.version = "1.2.4"

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_installed_record_mismatch",
    ):
        build_installed_artifact_manifest(
            _source_lock_for_installed_test(),
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )

    distribution.version = "1.2.3"
    distribution.entry_points[0].value = "acme_ext:other"
    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_entry_point_mismatch",
    ):
        build_installed_artifact_manifest(
            _source_lock_for_installed_test(),
            platform_tag="py3-none-any",
            find_distribution=lambda _name: distribution,
        )


def test_local_source_change_fails_before_installed_distribution_lookup(
    tmp_path: Path,
) -> None:
    backend = tmp_path / "backend"
    source = backend / "extensions" / "sources" / "acme-ext"
    source.mkdir(parents=True)
    (source / "pyproject.toml").write_text(
        '[project]\nname = "acme-ext"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )
    module = source / "acme_ext"
    module.mkdir()
    (module / "__init__.py").write_text("value = 1\n", encoding="utf-8")
    (backend / "pyproject.toml").write_text(
        '[dependency-groups]\nextensions = ["acme-ext"]\n',
        encoding="utf-8",
    )
    (backend / "uv.lock").write_text(
        """\
version = 1

[[package]]
name = "acme-ext"
version = "1.2.3"
source = { directory = "extensions/sources/acme-ext" }
""",
        encoding="utf-8",
    )
    lock = build_source_lock(
        backend,
        plugins=(
            {
                "name": "policy",
                "package": "acme-ext",
                "use": "acme_ext:install",
            },
        ),
        extension_api_version="0.13.0",
    )
    (module / "__init__.py").write_text("value = 2\n", encoding="utf-8")

    with pytest.raises(
        ExtensionArtifactVerificationError,
        match="extension_artifact_digest_mismatch",
    ):
        build_installed_artifact_manifest(
            lock,
            backend_dir=backend,
            platform_tag="py3-none-any",
            find_distribution=lambda _name: (_ for _ in ()).throw(AssertionError("distribution lookup preceded local source verification")),
        )

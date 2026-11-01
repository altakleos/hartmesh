"""The shared token shape and tracked-text credential hygiene, without live tokens."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from app.telegram_bots.adapter import TOKEN_BODY

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_web_and_server_share_one_token_body():
    source = (REPO_ROOT / "frontend-hm/src/core/telegram-bots/token.ts").read_text(encoding="utf-8")
    assert re.search(r'export const TOKEN_BODY = "(.+)";', source).group(1) == TOKEN_BODY


def _synthetic_token(digits: int = 9, secret_chars: int = 36) -> str:
    return "1" * digits + ":" + "A" * secret_chars


def _tracked_token_locations(root: Path) -> list[tuple[str, int]]:
    names = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True).stdout.split(b"\0")
    pattern = re.compile(r"(?<![0-9])" + TOKEN_BODY)
    found = []
    for name in names:
        if not name:
            continue
        relative = name.decode("utf-8")
        path = root / relative
        if path.is_symlink() or not path.is_file():
            continue
        raw = path.read_bytes()
        if b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        match = pattern.search(text)
        if match:
            # Credential text must never enter assertion output or CI logs.
            found.append((relative, text.count("\n", 0, match.start()) + 1))
    return found


def test_no_tracked_file_holds_a_bot_token() -> None:
    if shutil.which("git") is None:
        pytest.skip("tracked-text check requires git")
    checkout = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if checkout.returncode or Path(checkout.stdout.strip()).resolve() != REPO_ROOT:
        pytest.skip("tracked-text check requires the source checkout")
    assert _tracked_token_locations(REPO_ROOT) == []


def test_token_body_is_a_plain_unanchored_pattern() -> None:
    assert TOKEN_BODY == "[0-9]{6,12}:[A-Za-z0-9_-]{30,}"


def test_token_inside_an_api_address_is_found() -> None:
    token = _synthetic_token()
    address = "https://api.telegram.org/bot" + token + "/getMe"
    found = re.search(r"(?<![0-9])" + TOKEN_BODY, address)
    assert found is not None and found.group() == token
    assert re.search(r"\b" + TOKEN_BODY, address) is None


@pytest.mark.parametrize("digits,secret_chars,accepted", [(5, 36, False), (6, 30, True), (12, 30, True), (13, 36, False), (9, 29, False)])
def test_token_length_bounds_and_digit_prefix_are_respected(digits: int, secret_chars: int, accepted: bool) -> None:
    assert bool(re.search(r"(?<![0-9])" + TOKEN_BODY, _synthetic_token(digits, secret_chars))) is accepted


def test_secret_accepts_the_documented_url_safe_characters() -> None:
    token = "123456" + ":" + "aZ0_-" * 6
    assert re.fullmatch(TOKEN_BODY, token)


@pytest.mark.skipif(shutil.which("git") is None, reason="tracked-text fixture requires git")
def test_tracked_text_scan_ignores_untracked_ignored_and_binary_files(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
    (tmp_path / "safe.txt").write_text("ordinary text\n", encoding="utf-8")
    (tmp_path / "binary.dat").write_bytes(b"\x00" + _synthetic_token().encode("ascii"))
    (tmp_path / "non_utf8.dat").write_bytes(b"\xff" + _synthetic_token().encode("ascii"))
    (tmp_path / "untracked.txt").write_text(_synthetic_token(), encoding="utf-8")
    (tmp_path / "ignored.txt").write_text(_synthetic_token(), encoding="utf-8")
    subprocess.run(["git", "add", ".gitignore", "safe.txt", "binary.dat", "non_utf8.dat"], cwd=tmp_path, check=True)
    assert _tracked_token_locations(tmp_path) == []
    (tmp_path / "safe.txt").write_text("first line\n" + _synthetic_token(), encoding="utf-8")
    assert _tracked_token_locations(tmp_path) == [("safe.txt", 2)]

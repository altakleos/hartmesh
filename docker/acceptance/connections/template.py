"""Set the typed connection option in a disposable hosted operator template."""

import argparse
from pathlib import Path

import yaml


def apply_option(path: Path, enabled: bool) -> None:
    contents = yaml.safe_load(path.read_text(encoding="utf-8"))
    option = contents.setdefault("agent_connections", {"packages": []})
    option["enabled"] = enabled
    path.write_text(yaml.safe_dump(contents, allow_unicode=True, sort_keys=False), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["on", "off"])
    apply_option(Path("/opt/hartmesh/config.yaml"), parser.parse_args().mode == "on")

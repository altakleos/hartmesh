"""Write synthetic inputs for the opt-in authenticated tenant bundle browser test."""

import argparse
import json
from pathlib import Path

from PIL import Image


def prepare(directory: Path, *, configured_product: bool = False) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("Fixture directory must be empty")
    for child in ("tenant", "product/assets", "home", "data", "skills"):
        (directory / child).mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (40, 40), "orange").save(directory / "tenant/tenant.png")
    for name, color in (("product", "navy"), ("provider", "green"), ("icon", "navy")):
        Image.new("RGB", (32, 32), color).save(directory / f"product/assets/{name}.png")

    support_url = "https://support.example.test/help"
    documents = {
        "tenant/brand.json": {"company_name": "Example Tenant One", "logo": "tenant.png", "colors": {"primary": "#123456", "secondary": "#abcdef"}},
        "tenant/provider.json": {"display_name": "Example Provider", "support_url": support_url},
        "product/product.json": {
            "schema_version": 1,
            "public": True,
            "product": {"name": "Example Product", "logo": "assets/product.png", "browser_icon": "assets/icon.png"},
            "provider": {"name": "Example Provider", "logo": "assets/provider.png", "support_url": support_url},
        },
        "config.yaml": {
            "models": [],
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "database": {"backend": "sqlite", "sqlite_dir": str(directory / "data")},
            "ui": {"profile": "business"},
            "memory": {"enabled": False},
            "skills": {"path": str(directory / "skills")},
            "tenant_bundle": {"path": str(directory / "tenant")},
        },
        "extensions.json": {"mcpServers": {}, "skills": {}},
    }
    if configured_product:
        documents.pop("tenant/provider.json")
    for name, document in documents.items():
        (directory / name).write_text(json.dumps(document), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--configured-product", action="store_true")
    arguments = parser.parse_args()
    prepare(arguments.directory.resolve(), configured_product=arguments.configured_product)

"""Immutable locale catalogs; keyboards join when their delivery flows exist."""

from collections.abc import Callable, Mapping
from types import MappingProxyType

from . import en, zh_cn

_CATALOGS = {"en": MappingProxyType(en.TEXTS), "zh-CN": MappingProxyType(zh_cn.TEXTS)}

FIRST_KEYS: frozenset[str] = frozenset({"hello.privacy", "link.conflict", "link.conflict_over.chat", "link.conflict_over.custom_agent", "link.back_online", "link.removed", "access.off", "access.back", "delivery.failed"})

# Builders are introduced with their callback flows, starting in change 1.13.
KEYBOARDS: dict[str, Callable] = {}


def _locale(locale: str) -> str:
    return "zh-CN" if locale.lower().replace("_", "-") == "zh-cn" else "en"


def catalog(locale: str) -> Mapping[str, str]:
    """Return the fixed catalog, falling back to English for unknown locales."""
    return _CATALOGS[_locale(locale)]


def forms(locale: str) -> Mapping[str, Mapping[str, str]]:
    return zh_cn.FORMS if _locale(locale) == "zh-CN" else en.FORMS


def words(locale: str) -> Mapping[str, str]:
    return zh_cn.WORDS if _locale(locale) == "zh-CN" else en.WORDS

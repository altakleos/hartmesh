"""Host-issued qualification of one actual terminal-presentation graph.

This leaf deliberately imports no graph, runtime, provider or extension module.
Only the first-party assembly qualifier issues profiles; serialized claims and
lookalike objects cannot supply one to the worker.
"""

from dataclasses import dataclass, field

_ISSUER = object()


@dataclass(frozen=True, slots=True, init=False, eq=False)
class TerminalPresentationProfile:
    _graph: object = field(repr=False)
    _state_schema: object = field(repr=False)
    _middlewares: tuple[object, ...] = field(repr=False)
    _tools: tuple[object, ...] = field(repr=False)
    _middleware_tools: tuple[object, ...] = field(repr=False)
    _runtime_delivery: object | None = field(repr=False)
    _state_keys: frozenset[str] = field(repr=False)
    _presentation_state_keys: frozenset[str] = field(repr=False)
    _issuer: object = field(repr=False)

    def __new__(cls, *args, **kwargs):
        raise TypeError("Terminal presentation profiles are issued by the host")

    def accepts(self, graph: object) -> bool:
        return type(self) is TerminalPresentationProfile and getattr(self, "_issuer", None) is _ISSUER and getattr(self, "_graph", None) is graph

    @property
    def has_runtime_delivery(self) -> bool:
        return getattr(self, "_runtime_delivery", None) is not None

    @property
    def state_keys(self) -> frozenset[str]:
        return self._state_keys

    @property
    def presentation_state_keys(self) -> frozenset[str]:
        return self._presentation_state_keys


def _issue_terminal_presentation_profile(
    graph: object,
    *,
    state_schema: object,
    middlewares: tuple[object, ...],
    tools: tuple[object, ...],
    middleware_tools: tuple[object, ...],
    runtime_delivery: object | None,
    state_keys: frozenset[str],
    presentation_state_keys: frozenset[str],
) -> TerminalPresentationProfile:
    """Called only after the assembly checks its actual first-party adapters."""
    if graph is None or not state_keys or any(type(key) is not str for key in state_keys) or not presentation_state_keys <= state_keys or runtime_delivery is not None and not any(item is runtime_delivery for item in middlewares):
        raise ValueError("Invalid terminal presentation assembly")
    profile = object.__new__(TerminalPresentationProfile)
    for name, value in (
        ("_graph", graph),
        ("_state_schema", state_schema),
        ("_middlewares", middlewares),
        ("_tools", tools),
        ("_middleware_tools", middleware_tools),
        ("_runtime_delivery", runtime_delivery),
        ("_state_keys", state_keys),
        ("_presentation_state_keys", presentation_state_keys),
        ("_issuer", _ISSUER),
    ):
        object.__setattr__(profile, name, value)
    return profile

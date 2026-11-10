"""Source-bound, button-free blocked-site trailers after an acknowledged reply."""


def prepare_notice(record, source, parent_id):
    from app.telegram_bots.inbox import publication_identity
    from app.telegram_bots.network_denials import notice_hosts
    from app.telegram_bots.producer import _safe_json

    hosts = notice_hosts(record, source)
    if not hosts or type(parent_id) is not str or len(parent_id) != 32 or any(char not in "0123456789abcdef" for char in parent_id):
        raise ValueError("network-notice-source-conflict")
    parts = [{"kind": "text", "source_ref": {"key": "net.blocked_ask", "values": {"host": host}}} for host in hosts]
    outcome = {"kind": "network_denials", "denial_basis": source, "projection": source["projection"]}
    return _safe_json({"version": 8, "publication": publication_identity(record), "outcome": outcome, "parent_id": parent_id, "parts": parts, "finish_route": False}), parts


def validate_notice(item, part=None):
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope
    from app.telegram_bots.model_source import VerifiedModelReplyBasis
    from app.telegram_bots.producer import OutboxItem, _canonical_primary

    try:
        required = {"source", "reply_basis", "presentation_version"}
        if type(item) is not OutboxItem or type(item.payload) is not dict or not required <= set(item.payload) or set(item.payload) - required - {"replacement_reason"}:
            raise ValueError
        basis = item.payload["reply_basis"]
        if type(basis) is not dict or set(basis) != {"version", "publication", "outcome", "parent_id", "parts", "finish_route"} or type(basis["version"]) is not int or basis["version"] != 8 or basis["finish_route"] is not False:
            raise ValueError
        outcome = basis["outcome"]
        if type(outcome) is not dict or set(outcome) != {"kind", "denial_basis", "projection"} or outcome["kind"] != "network_denials":
            raise ValueError
        output = RunOutputProjectionV2.model_validate(outcome["projection"])
        identity = basis["publication"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": True, "run_id": output.output.run_id})
        expected, initial = prepare_notice(record, outcome["denial_basis"], basis["parent_id"])
        if _canonical_primary(basis) != _canonical_primary(expected) or _canonical_primary(item.payload["source"]) != _canonical_primary(outcome):
            raise ValueError
        if (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.class_, item.dedupe_key) != (
            scope.link_id,
            scope.owner_user_id,
            scope.binding_generation,
            scope.chat_id,
            "reply",
            "notice:network-denials:" + record.publication_id,
        ):
            raise ValueError
        if item.message_thread_id not in (None, scope.message_thread_id):
            raise ValueError
        version = item.payload["presentation_version"]
        if type(version) is not int or version < 0 or version > len(item.parts):
            raise ValueError
        if version and item.payload.get("replacement_reason") not in {"topic-missing", "protection", "edit-anchor-missing"}:
            raise ValueError
        if not version and "replacement_reason" in item.payload:
            raise ValueError
        runtime = {"part_id", "presentation_version", "ordinal", "state", "message_ids", "sent_at", "dispatched_at", "protect_content", "sent_protect_content", "plain_fallback", "invalid_token_verification_pending", "callback_group_id"}

        def original(value):
            if type(value) is not dict or type(value.get("presentation_version")) is not int or not 0 <= value["presentation_version"] <= version:
                raise ValueError
            if value["presentation_version"] < version and value.get("state") not in {"sent", "retired"}:
                raise ValueError
            if value.get("callback_group_id") is not None:
                raise ValueError
            if any(type(value[key]) is not bool for key in ("plain_fallback", "protect_content", "sent_protect_content") if key in value):
                raise ValueError
            return {key: child for key, child in value.items() if key not in runtime}

        originals = {value["part_id"]: original(value) for value in item.parts}
        if len(originals) != len(item.parts):
            raise ValueError
        acknowledged = set()
        for generation in range(version + 1):
            current = [value for value in item.parts if value["presentation_version"] == generation]
            remaining = [value for value in initial if _canonical_primary(value["source_ref"]) not in acknowledged]
            if not current or _canonical_primary({"parts": [originals[value["part_id"]] for value in current]}) != _canonical_primary({"parts": remaining}):
                raise ValueError
            acknowledged.update(_canonical_primary(value["source_ref"]) for value in current if value.get("state") == "sent")
        if part is not None and not any(_canonical_primary(part) == _canonical_primary(value) for value in item.parts):
            raise ValueError
        return VerifiedModelReplyBasis(record, output, None, "network_denials")
    except Exception:
        raise ValueError("network-notice-source-conflict") from None


def parent_ready(item, parent, *, state):
    from app.telegram_bots.inbox import publication_identity
    from app.telegram_bots.model_source import validate_model_reply_basis
    from app.telegram_bots.producer import _canonical_primary

    proof = validate_notice(item)
    if parent is None or parent.id != item.payload["reply_basis"]["parent_id"] or parent.dedupe_key != "reply:" + proof.record.turn_id:
        return False
    primary = validate_model_reply_basis(parent)
    return (
        _canonical_primary(publication_identity(primary.record)) == _canonical_primary(publication_identity(proof.record))
        and primary.output == proof.output
        and state == "sent"
        and any(value.get("state") == "sent" and value.get("message_ids") for value in parent.parts)
    )


async def add_notice(outbox, record, source, parent_id, *, writer, link):
    from app.telegram_bots.network_denials import checked_denials

    basis, parts = prepare_notice(record, checked_denials(source, record), parent_id)
    await source._guard.check(writer)
    identity, created = await outbox._add(
        link, dedupe_key="notice:network-denials:" + record.publication_id, class_="reply", payload={"source": basis["outcome"], "reply_basis": basis, "parts": parts}, session=writer, message_thread_id=record.scope.message_thread_id
    )
    if not created:
        raise ValueError("network-notice-source-conflict")
    return identity


async def read_notice(source, private, proof):
    from app.telegram_bots.model_source import PreparedModelPart
    from app.telegram_bots.network_denials import checked_denials
    from app.telegram_bots.producer import SourceGone, _canonical_primary, _item
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    if source._outbox is None or source._outbox.network_denials is None:
        raise SourceGone
    qualified = await source._outbox.network_denials.read(proof.record, private.link, output=proof.output)
    if qualified is None or _canonical_primary(checked_denials(qualified, proof.record)) != _canonical_primary(private.item.payload["reply_basis"]["outcome"]["denial_basis"]):
        raise SourceGone
    async with source._outbox.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, private.item.payload["reply_basis"]["parent_id"])
        if parent is None or not parent_ready(private.item, _item(parent), state=parent.state):
            raise SourceGone
    return PreparedModelPart("", "", network_denial_host=private.part["source_ref"]["values"]["host"])


async def notice_eligibility(authority, link, item):
    from app.telegram_bots.producer import DeliveryEligibility, _item
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    validate_notice(item)
    async with authority.links.session_factory() as reader:
        row = await reader.get(TelegramOutboxRow, item.payload["reply_basis"]["parent_id"])
        if row is None or row.state == "skipped":
            return DeliveryEligibility.skip("superseded")
        parent = _item(row)
    return None if parent_ready(item, parent, state=row.state) else DeliveryEligibility.wait()

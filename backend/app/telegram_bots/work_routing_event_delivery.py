"""Dormant manager notification materialization with exact response ownership."""

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_event_delivery import WorkEventDelivery, _MaterializationProjection
from app.telegram_bots.work_event_facts import event_response
from app.telegram_bots.work_event_source import require_event_source
from app.telegram_bots.work_routing_authority import RoutingCandidates
from app.telegram_bots.work_routing_recovery import WorkRoutingRecovery, _RoutingRecoveryProjection


class WorkRoutingEventDelivery(WorkEventDelivery):
    keys = {"request.unrouted": None}
    actions = {"request_input", "revise_input", "human_response", "attempt_settled"}

    def __init__(self, plans):
        super().__init__(plans)
        self.recovery = WorkRoutingRecovery(plans)

    async def _qualify_event(self, writer, event, intent, request, receipt):
        if event.action == "human_response":
            response = await event_response(writer, event, request, receipt)
            if response.id != intent.get("response_id") or response.disposition not in {"cannot_provide", "wrong_recipient"}:
                raise ProductSourceRetired()
        elif intent.get("response_id") is not None:
            raise ProductSourceRetired()

    async def _freeze(self, writer, link, row, source, settings):
        await self.plans.freeze_routing(writer, link, row, source.value, settings, source.public_base_url)

    def _projection(self, source, item):
        return _RoutingMaterializationProjection(self.outbox.sessions, source, item)


class _RoutingMaterializationProjection(_MaterializationProjection):
    def __init__(self, sessions, source, item):
        super().__init__(source, item)
        self.candidates = RoutingCandidates(sessions, source.link, source.value.source, source.routing)

    def extend(self, query):
        return self.candidates.extend(super().extend(query))

    def check(self, rows):
        from app.telegram_bots.producer import _item

        _RoutingRecoveryProjection.check(self, rows)
        delivery, event, receipt = rows[0][11:14]
        if delivery.claim_id is not None or _item(delivery) != self.item:
            raise ProductSourceRetired()
        require_event_source(self.item, self.source, event=event, receipt=receipt, final=True)

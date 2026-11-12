"""Materialize canonical review events using the result media owner."""

from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_event_delivery import WorkEventDelivery
from app.telegram_bots.work_event_source import require_event_source
from app.telegram_bots.work_result_recovery import WorkResultRecovery, _ResultRecoveryProjection
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


class WorkReviewEventDelivery(WorkEventDelivery):
    keys = {"review.ready": "review"}
    actions = {"attempt_settled", "request_route"}

    def __init__(self, plans):
        super().__init__(plans)
        self.recovery = WorkResultRecovery(plans)
        self.plans = self.recovery.plans

    async def _freeze(self, writer, link, row, source, settings):
        if row.payload["work_event_intent"].get("attempt_id") != source.work["outcome"]["attempt_id"]:
            raise ProductSourceRetired()
        await super()._freeze(writer, link, row, source, settings)

    def _projection(self, source, item):
        return _ReviewMaterializationProjection(source, item)


class _ReviewMaterializationProjection(_ResultRecoveryProjection):
    def __init__(self, source, item):
        super().__init__(source)
        self.item = item

    def extend(self, query):
        result = super().extend(query)
        self.event_column = len(result.column_descriptions)
        delivery, event, receipt = TelegramOutboxRow, AgentWorkEventRow, HumanInputEventRow
        return (
            result.add_columns(delivery, event, receipt)
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(event, event.id == delivery.event_id)
            .outerjoin(receipt, (receipt.instance_id == event.instance_id) & (receipt.operation_id == event.operation_id))
        )

    def check(self, rows):
        super().check(rows)
        delivery, event, receipt = rows[0][self.event_column : self.event_column + 3]
        if delivery.claim_id is not None or _item(delivery) != self.item:
            raise ProductSourceRetired()
        require_event_source(self.item, self.source, event=event, receipt=receipt, final=True)

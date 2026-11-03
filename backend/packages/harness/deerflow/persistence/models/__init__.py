"""ORM model registration entry point.

Importing this module ensures all ORM models are registered with
``Base.metadata`` so Alembic autogenerate detects every table.

The actual ORM classes have moved to entity-specific subpackages:
- ``deerflow.persistence.thread_meta``
- ``deerflow.persistence.run``
- ``deerflow.persistence.feedback``
- ``deerflow.persistence.user``

``RunEventRow`` remains in ``deerflow.persistence.models.run_event`` because
its storage implementation lives in ``deerflow.runtime.events.store.db`` and
there is no matching entity directory.
"""

from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputReadRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow, AgentMemoryRow, AgentProtectedContextRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.agents.model import AgentRow
from deerflow.persistence.channel_connections.model import (
    ChannelConnectionRow,
    ChannelConversationRow,
    ChannelCredentialRow,
    ChannelOAuthStateRow,
)
from deerflow.persistence.channel_leases.model import ChannelLeaseRow
from deerflow.persistence.channel_runtime_configs.model import ChannelRuntimeConfigRow
from deerflow.persistence.channel_thread_bindings.model import ChannelThreadBindingRow
from deerflow.persistence.extension_leases.model import ExtensionExecutionLeaseRow
from deerflow.persistence.feedback.model import FeedbackRow
from deerflow.persistence.login_throttle.model import LoginThrottleRow
from deerflow.persistence.managed_subagents.model import ManagedSubagentRow
from deerflow.persistence.mcp_oauth_tokens.model import McpOAuthTokenRow
from deerflow.persistence.mcp_tasks.model import McpTaskRow
from deerflow.persistence.models.run_event import RunEventRow, RunEventThreadSeqRow
from deerflow.persistence.notification_deliveries.model import NotificationDeliveryRow
from deerflow.persistence.personal_access_tokens.model import PersonalAccessTokenRow
from deerflow.persistence.projects.model import ProjectDocumentRow, ProjectRow
from deerflow.persistence.provider_keys.model import ProviderKeyEventRow, ProviderKeyRow
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.scheduled_task_events.model import ScheduledTaskEventRow
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow
from deerflow.persistence.shared_publications.model import SharedPublicationRow
from deerflow.persistence.skill_mutations.model import SkillAssetRow, SkillOperationRow, SkillOwnerRow, SkillProposalRow
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceBackupRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.persistence.subagent_batches.model import SubagentBatchItemRow, SubagentBatchRow
from deerflow.persistence.telegram_bots import (
    TelegramAccountRow,
    TelegramActRow,
    TelegramAssistantAddRow,
    TelegramBotIntentRow,
    TelegramBotLinkEventRow,
    TelegramBotLinkRow,
    TelegramCallbackRow,
    TelegramDeploymentRow,
    TelegramInboundMarkRow,
    TelegramInstancePolicyRow,
    TelegramOutboxRow,
    TelegramPendingFileRow,
    TelegramPersonSettingsRow,
    TelegramWorkCursorRow,
    TelegramWorkTopicRow,
)
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_reads.model import ThreadReadMarkerRow, ThreadReadVersionRow
from deerflow.persistence.user.access import DisabledIdentityRow, IdentityHoldRow, RoleLimitRow
from deerflow.persistence.user.im_notifications import ImNotificationPreferenceRow
from deerflow.persistence.user.model import UserPreferenceRow, UserRow
from deerflow.persistence.webhook_delivery.model import WebhookDeliveryRow

__all__ = [
    "HumanInputEventRow",
    "HumanInputReadRow",
    "HumanInputRequestRow",
    "HumanInputResponseRow",
    "AgentWorkRow",
    "AgentWorkAttemptRow",
    "AgentWorkEventRow",
    "AgentConversationRow",
    "AgentDefinitionRevisionRow",
    "AgentInstanceGrantRow",
    "AgentInstanceRow",
    "AgentLifecycleRow",
    "AgentMemoryRow",
    "AgentProtectedContextRow",
    "ExtensionExecutionLeaseRow",
    "AgentRow",
    "ChannelConnectionRow",
    "ChannelConversationRow",
    "ChannelCredentialRow",
    "ChannelLeaseRow",
    "ChannelOAuthStateRow",
    "TelegramBotLinkRow",
    "TelegramBotLinkEventRow",
    "TelegramAccountRow",
    "TelegramPersonSettingsRow",
    "TelegramBotIntentRow",
    "TelegramInstancePolicyRow",
    "TelegramAssistantAddRow",
    "TelegramWorkTopicRow",
    "TelegramCallbackRow",
    "TelegramActRow",
    "TelegramWorkCursorRow",
    "TelegramOutboxRow",
    "TelegramInboundMarkRow",
    "TelegramPendingFileRow",
    "TelegramDeploymentRow",
    "ChannelRuntimeConfigRow",
    "ChannelThreadBindingRow",
    "FeedbackRow",
    "LoginThrottleRow",
    "McpOAuthTokenRow",
    "McpTaskRow",
    "ManagedSubagentRow",
    "NotificationDeliveryRow",
    "PersonalAccessTokenRow",
    "ProjectDocumentRow",
    "ProjectRow",
    "ProviderKeyEventRow",
    "ProviderKeyRow",
    "RunEventRow",
    "RunEventThreadSeqRow",
    "RunChangeClockRow",
    "RunRow",
    "SkillAssetRow",
    "SkillOperationRow",
    "SkillOwnerRow",
    "SkillProposalRow",
    "ScheduledTaskEventRow",
    "ScheduledTaskRow",
    "ScheduledTaskRunRow",
    "SharedPublicationRow",
    "StorageFeatureLinkRow",
    "SpaceRow",
    "SpaceGrantRow",
    "SpaceEventRow",
    "SpaceBackingRow",
    "SpaceFileOperationRow",
    "SpaceAttachmentRow",
    "SpaceMountRow",
    "SpaceBackupRow",
    "StorageHostReplacementRow",
    "SubagentBatchRow",
    "SubagentBatchItemRow",
    "ThreadMetaRow",
    "ThreadReadMarkerRow",
    "ThreadReadVersionRow",
    "UserPreferenceRow",
    "DisabledIdentityRow",
    "IdentityHoldRow",
    "RoleLimitRow",
    "ImNotificationPreferenceRow",
    "UserRow",
    "WebhookDeliveryRow",
]

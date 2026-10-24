# Managed Telegram bots

`config.py` validates the optional `AppConfig.model_extra["telegram_bots"]`
section without starting services. Nested models forbid unknown keys and type
coercion; absence returns fresh defaults with `enabled=False`. Keep this separate
from the existing deployment-wide channel adapter.

Compose's renderer owns enabled/public URL/bot limits/voice provider and the
legacy-channel closure. Until stage one is complete, an absent environment key
omits the section and `show_web_page` tool. See `deploy/compose/README.md`.

Keep bot network, supervisor and router startup outside configuration loading.
Use socket-free transports and synthetic credentials in offline tests.

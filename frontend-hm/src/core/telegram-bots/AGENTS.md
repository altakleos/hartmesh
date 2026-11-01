# Telegram client

`core/telegram-bots/` is the client of `/api/telegram-bots`;
`components/workspace/telegram/` draws it.

`useTelegram` fails closed: a null, missing or unreadable features key renders
nothing about Telegram. `rowOf` in `rows.ts` is the one row table, and every place
that shows a bot's state reads it. `setupStageOf` derives the set-up's stage from
the list, so a reload or a second tab agrees; keep no second copy of it.
`useTelegramClock` re-renders what changes with time alone, because an unchanged
poll renders nothing.

Refusals are shown by `reason` through `telegramRefusalKey`, never by `detail`.
Texts live under `telegramBots` as dotted keys, and `telegramText` resolves them. A
text that says whom to ask takes the reader's role, because no text sends a user to
the provider.

`TOKEN_BODY`, `pictureOf`, `displayNameOf` and `suggestUsername` mirror server rules
and are pinned on both sides.

Tokens: keep the two calls that carry a token as plain `api.ts` calls, never
mutations, because the query library would keep the token as its variables. Call
`findToken` on a paste, a press or a blur, never while typing. `TelegramTokenField`
is the one paste rule for both token dialogs. `TelegramTokenDialog` keeps one
`operation_id` per token, by a lossy fingerprint, so a retry after a lost answer
returns the saved receipt. Consent to take a webhook over covers the refused token's
fingerprint only.

`setup-dialog-store.ts` holds what only this page knows: the open set-up, the
connect addresses it received, and which links answer a pasted token. A held
address is stored with `telegramConnectEpoch`, so an answer to a request made before
Disconnect all is dropped.

`?settings=channels&telegram=<target>` opens a row; a target uses a dot because the
sign-in return refuses a colon. `/workspace` passes its query on when it redirects,
which every link from a bot depends on. The workspace layout sends a signed-out
person to sign-in from the browser through `RedirectToLogin`, because a layout
receives no query. `qrcode.react` is imported only by `qr-code.tsx`, behind the
dynamic set-up host, so it stays out of the workspace's first load.

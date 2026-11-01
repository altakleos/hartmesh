"""English fixed texts for stage-one managed Telegram bots."""

TEXTS: dict[str, str] = {
    "hello.chat": "Connected as {email}. This chat is {product}. No other Telegram account can write here.\n"
    "\n"
    "Ask anything or send a file. Everything here is also kept in {product} on the web.\n"
    "\n"
    "Answers here come from an AI and can be wrong. Check anything you will act on.",
    "hello.custom_agent": "Connected as {email}. This chat is {agent}, one of your custom agents in {product}. No other Telegram account can write here.\n"
    "\n"
    "Ask anything or send a file. Everything here is also kept in {product} on the web.\n"
    "\n"
    "{agent} is an AI and can be wrong. Check anything you will act on.",
    "hello.privacy": "This chat goes through Telegram, which can read it.\nSend /privacy for the details.",
    "hello.link_expired": "This connect link no longer works. Open your web app and get a new link for this bot.",
    "hello.refused": "This link belongs to another bot, so nothing was connected. Open your web app and use the link shown for this bot.",
    "hello.account_taken": "This Telegram account is already connected to another account. Disconnect it there first.",
    "hello.again": "This chat is already connected. /help shows what it can do.",
    "stranger": "This is a private bot that answers only the person who connected it.",
    "stranger.waiting": "This bot is not connected yet. If it is yours, use the Open chat button in your web app, then press Start.",
    "help.chat": "This is your {product} bot.\n"
    "\n"
    "In this chat:\n"
    "- write, or send a file, a photo or a voice note\n"
    "- /new starts a new conversation\n"
    "- /stop stops the current answer\n"
    "- /settings sets the language\n"
    "- a skill's name after a slash uses that skill for one message",
    "help.custom_agent": "This is {agent}, one of your custom agents in {product}.\n"
    "\n"
    "In this chat:\n"
    "- write, or send a file, a photo or a voice note\n"
    "- /new starts a new conversation\n"
    "- /stop stops the current answer\n"
    "- a skill's name after a slash uses that skill for one message",
    "privacy.full": "About this chat and Telegram\n"
    "\n"
    "Everything sent here, including files and voice notes, passes through Telegram and stays in your Telegram account until you delete it. Bot chats are not "
    "end-to-end encrypted, so Telegram can read them, and {product} cannot delete them for you.\n"
    "\n"
    "This bot answers only the Telegram account that connected it. A notification can show the start of a message on a locked phone; Telegram and your phone can hide "
    "previews.\n"
    "\n"
    "Anyone who can open your Telegram can write here as you. Use a screen lock. If your phone is lost, end that session from another device (Telegram, Settings, "
    "Devices) and disconnect your bots in {product} on the web.\n"
    "\n"
    "Never send a bot token or a password here.",
    "privacy.voice": "Voice notes are turned into text by {transcriber}, after you agree to that service in the chat. If the service changes, you are asked again. {product} does not keep the recording.",
    "activity.read": "Reading {file}…",
    "activity.write": "Writing {file}…",
    "activity.edit": "Editing {file}…",
    "activity.list": "Looking through the files…",
    "activity.command": "Carrying out a step…",
    "activity.search": "Searching the web…",
    "activity.fetch": "Reading {host}…",
    "activity.browser": "Using the browser…",
    "activity.image": "Looking at {file}…",
    "activity.subtask": "Working on a part of this…",
    "activity.present": "Preparing the files…",
    "activity.work": "Checking the assignment…",
    "activity.other": "Working…",
    "answer.thinking": "Thinking…",
    "busy.followup": "Still answering. This message is next, and anything you add before then is answered with it.",
    "busy.followup_full": "Ten messages are already waiting. Send this one again after the answer.",
    "busy.queued": "Too much is running at once. Your message is in line and will be answered within two minutes. No need to send it again.",
    "busy.cancelled": "Taken out of line. Nothing was answered.",
    "busy.gave_up": "Still busy after two minutes, so your message was not answered. It is kept.",
    "busy.thread": "This conversation is busy with an answer that was started on the web. Your message is in line.",
    "error.generic": "Something went wrong and the message was not answered. It is kept.",
    "error.model_refused": "The AI service refused this request, so the message was not answered. Ask {support} to check it.",
    "answer.on_web": "This answer cannot be shown here. It is in {product} on the web.",
    "refused.interrupted": "{name}'s workspace was restarted, which stopped this answer. Your message is kept.",
    "run.cancelled": "This request was cancelled.",
    "chat.restarted": "The service restarted before this was answered. Your message is kept.",
    "chat.files_full": "Your files are full, so a new conversation cannot start. Delete files or old conversations in {product} on the web, then try again. If that is not enough, ask {support} for more room. Your message is kept.",
    "stop.answer": "Stopped. The text above is unfinished.",
    "stop.nothing": "There is nothing to stop.",
    "stop.failed": "Stopping did not work. Send /stop again.",
    "file.saved_ask_plain": "Received {file}. What should be done with it?",
    "file.saved_ask_photo": "Received your photo. What should be done with it?",
    "file.saved_ask_many": "Received {n} files. What should be done with them?",
    "file.too_large_in": "That file is over 20 MB, the most a Telegram bot can receive. Add it in {product} on the web.",
    "file.not_saved": "{file} could not be saved.",
    "file.own_full": "Your files are full, so {file} was not saved. Delete files or old conversations in {product} on the web, then send {file} again. If that is not enough, ask {support} for more room.",
    "file.own_preparing": "Your files are being prepared, so {file} was not saved, and this chat runs without files for now. If this lasts longer than an hour, ask {support}.",
    "file.own_no_seat": "Files are not available for your account, so {file} was not saved, and this chat runs without files. Ask an administrator.",
    "file.own_unchecked": "Your files did not pass the safety check, so {file} was not saved, and this chat runs without files. Everything in your files is kept.",
    "file.own_unavailable": "Files are not available right now, so {file} was not saved. This chat works without files until this is fixed.",
    "file.too_large_out": "{file} is over 50 MB, too large to send here. It is in {product} on the web.",
    "file.out_failed": "{file} is ready but could not be sent here.",
    "voice.unavailable": "Voice notes are not switched on in {product}. Type the message instead. The microphone key on your phone's keyboard turns speech into text.",
    "voice.too_long": "Voice notes of up to {n} minutes work here. Send a shorter one or type it.",
    "voice.daily_limit": "Today's {n} minutes of voice notes are used up. Type the message, or use voice again tomorrow.",
    "voice.first_notice": "Voice notes are turned into text by {transcriber}, one of the AI services {product} already uses here. The recording is sent to {transcriber}. {product} does not keep it. Use voice notes?",
    "voice.agreed": "Voice notes are on.",
    "voice.not_used": "Not turned into text. Type the message instead.",
    "voice.writing": "Writing down your voice note…",
    "voice.heard": 'Heard: "{text}"',
    "voice.failed": "The voice note could not be turned into text just now. Send it again or type it.",
    "voice.unclear": "That voice note could not be understood. Try again or type it.",
    "media.unsupported": "This chat takes text, voice notes, photos and files. That kind of message is not passed on. A video or a recording can be sent as a file.",
    "edit.ignored": "Edits are not picked up. Send the corrected text as a new message.",
    "token.pasted": "That looked like a bot token, so the message was deleted. Anyone who saw it could control that bot. Replace the token in BotFather with /revoke, then paste the new one in {product} on the web.",
    "token.pasted_kept": "That looked like a bot token. Delete that message, replace the token in BotFather with /revoke, and paste the new one in {product} on the web.",
    "clarify.options": "{text}",
    "clarify.more_choices": "For a choice without a button, tap Something else and type the choice.",
    "clarify.answer_line": "Your answer: {label}",
    "clarify.other_prompt": "Type your answer.",
    "clarify.form_field": "{n} of {total}: {label}",
    "clarify.form_yes": "Yes",
    "clarify.form_no": "No",
    "clarify.form_done": "Done",
    "clarify.form_review": "Send these answers?\n{label}: {value}",
    "net.ask_plain": "A step tried to reach {host}, which is blocked. Allow {host} for {minutes} minutes, or keep {host} blocked.",
    "net.allowed": "{host} is allowed for {minutes} minutes. The step is being tried again.",
    "net.kept": "{host} stays blocked.",
    "net.cancelled": "{host} stays blocked, because a new message was sent.",
    "net.stale": "This request is no longer waiting. Ask again and it will be retried.",
    "new.confirm_plain": "Start a new conversation? This one stays in {product} on the web, and the new one starts without what was said here.",
    "new.done": "New conversation started.",
    "new.kept": "Kept. This conversation goes on.",
    "new.in_topic": "/new works in the main chat, not in a topic.",
    "new.files_full": "Your files are full, so a new conversation cannot start. Delete files or old conversations in {product} on the web, then try again. If that is not enough, ask {support} for more room.",
    "cmd.unknown": "/{command} is not a command in this chat. Tap the menu button to see the commands.",
    "cmd.agent": "This chat is always {name}. Each custom agent has a separate chat.",
    "cmd.skill_off": "The skill /{skill} is switched off.",
    "cmd.skill_unavailable": "The skill /{skill} is not available in this chat.",
    "cmd.skill_failed": "The skill /{skill} could not be looked up just now. Send it again in a moment.",
    "settings.state_basic": "Language of this chat's fixed texts: {language}. Answers follow the language you write in.",
    "link.conflict": "This chat is paused.\nAnother program is using this bot, so messages may be missed. If that was not you, replace the bot's token: in BotFather send /revoke, then paste the new token in {product} on the web.",
    "link.conflict_over.chat": "This chat works again.\nAllowing a blocked site stays paused on this bot until you replace the token or confirm in {product} on the web that the other program was yours.",
    "link.conflict_over.custom_agent": "This chat works again.\nAllowing a blocked site stays paused on this bot until you replace the token or confirm in {product} on the web that the other program was yours.",
    "link.limited": "Paused on this bot since another program used it. Open {product} on the web and replace the token or confirm it was you.",
    "link.back_online": "This chat was offline for a long time.\nMessages may have been missed. Send again anything that was not answered.",
    "link.removed": "This bot was disconnected from {product}.\nThis chat will no longer answer. To remove this conversation from Telegram, delete the chat.",
    "access.off": "Your access to {product} has been turned off.\nThis chat will not answer. Ask your administrator. If the chat stays silent after your access is back, open {product} on the web and press Reconnect for this bot.",
    "access.back": "Your access to {product} is back.\nMessages sent while it was off were not received. Send them again.",
    "custom_agent.deleted": "{agent} no longer exists in {product}, so this chat cannot answer. Remove this bot on the web.",
    "update.refused": "That message could not be read, so it was not passed on. Send it another way.",
    "unavailable.ask_admin": "Ask an administrator.",
    "unavailable.next": "Ask {provider} to fix this.",
    "unavailable.reference": "Reference for support: {code}",
    "stale.other_person": "These buttons work only for the person who connected this chat.",
    "stale.not_waiting": "Nothing is waiting on this any more.",
    "tap.failed": "That did not go through. Tap again.",
    "form.required": "This one is required, so the form cannot go on without it.",
    "delivery.failed": "A saved message could not be delivered here. Retrying delivery will not start new work.",
    "prompt.closed": "This answer prompt closed. Reopen it to answer, send this as a new message, or discard it.",
    "prompt.unavailable": "This prompt can no longer take an answer. Send this as a new message, or discard it.",
    "prompt.discarded": "Discarded. Nothing was saved or sent.",
    "preview.part_header": "{heading} · {part}/{parts}",
    "profile.name.chat": "{product}",
    "profile.short.chat": "{product}. Private: answers only the person who connected this bot.",
    "profile.short.custom_agent": "Custom agent. Private: answers only the person who connected this bot.",
    "profile.description.chat": "This is a private {product} bot. Only the person who connected this bot gets an answer, in this chat.\n\nIf this is your bot, use the Open chat button in your web app, then press Start.",
    "profile.description.custom_agent": "This is a private custom agent. Only the person who connected this bot gets an answer, in this chat.\n\nIf this is your bot, use the Open chat button in your web app, then press Start.",
    "profile.removed": "This bot is no longer connected.",
    "web.list": "These are done in {product} on the web:",
    "web.memory": "What {name} remembers can be reviewed and corrected in {product} on the web.",
    "web.conversations": "All conversations with {name} are in {product} on the web.",
    "web.conversation": "This is easier to read on a larger screen.",
    "web.addAssistant": "AI assistants are added in {product} on the web.",
    "web.addEmployee": "AI employees are added in {product} on the web.",
    "web.telegram": "Bots are connected in {product} on the web, because a bot's token must never be typed into a chat.",
    "web.timezone": "The time zone is changed in {product} on the web.",
    "web.scheduled": "Scheduled tasks are set up in {product} on the web.",
    "web.account": "Your account and the administration are in {product} on the web.",
    "web.fileTooLarge": "That file is over 20 MB, the most a Telegram bot can receive. Add it in {product} on the web.",
    "web.resultTooLarge": "{file} is over 50 MB, too large to send here. It is in {product} on the web.",
    "label.stop": "Stop",
    "label.try_again": "Try again",
    "label.cancel": "Cancel",
    "label.send_again": "Send again",
    "label.use_voice": "Use voice notes",
    "label.not_now": "Not now",
    "label.something_else": "Something else",
    "label.skip": "Skip",
    "label.send": "Send",
    "label.change": "Change",
    "label.back": "Back",
    "label.allow_site": "Allow for {minutes} minutes",
    "label.keep_blocked": "Keep blocked",
    "label.start_new": "Start new",
    "label.keep_conversation": "Keep this one",
    "label.web_list": "What is done on the web",
    "label.english": "English",
    "label.chinese": "中文",
    "label.retry_delivery": "Retry delivery",
    "label.reopen_answer": "Reopen answer",
    "label.send_new": "Send as new message",
    "label.discard": "Discard",
    "label.open_memory": "Open Memory",
    "label.open_conversations": "Open conversations",
    "label.open_conversation": "Open conversation",
    "label.add_assistant": "Add AI assistant",
    "label.add_employee": "Add AI employee",
    "label.open_telegram": "Open Telegram settings",
    "label.open_scheduled": "Open scheduled tasks",
    "label.open_product": "Open {product}",
    "label.open_files": "Open Files",
    "label.form_field": "{label}",
    "label.choice": "{label}",
    "menu.new": "Start a new conversation",
    "menu.stop": "Stop the current answer",
    "menu.settings": "Language",
    "menu.help": "What this chat can do",
}

FORMS: dict[str, dict[str, str]] = {
    "file.own_no_seat": {"administrator": "Files are not available for your account, so {file} was not saved, and this chat runs without files. More accounts exist here than seats. Ask {provider} for more seats."}
}

WORDS = {"administrator": "an administrator", "manager": "a manager", "colleague": "a colleague", "provider": "your service provider"}

FORMS.update(
    {
        "stop.answer": {"unwritten": "Stopped.", "waiting": "Messages that were waiting were not answered."},
        "chat.restarted": {"no_message": "The service restarted before this was answered. Send it again."},
        "help.chat": {"no_voice": TEXTS["help.chat"].replace(", a photo or a voice note", " or a photo")},
        "help.custom_agent": {"no_voice": TEXTS["help.custom_agent"].replace(", a photo or a voice note", " or a photo")},
        "media.unsupported": {"no_voice": TEXTS["media.unsupported"].replace("voice notes, ", "")},
    }
)

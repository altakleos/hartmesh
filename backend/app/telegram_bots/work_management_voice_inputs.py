"""Spoken corrections reuse private prompt custody before transcription."""

from app.telegram_bots.assignment_voice_inputs import AssignmentVoiceInputs, check_voice_prompt, voice_basis
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body


class ManagementVoiceInputs(AssignmentVoiceInputs):
    owner = "management"
    purposes = frozenset({"work_edit"})
    intent_key = "management_prompt_intent"


def management_voice(link, mark, prompt, parent):
    """Re-prove original recording and full shown prompt, including on replay."""
    if "voice_note" not in mark.route_basis:
        if "work_voice" in mark.route_basis:
            raise ProductSourceRetired()
        return None
    value = voice_basis(mark, owner="management")
    if value is None:
        raise ProductSourceRetired()
    check_voice_prompt(link, mark, prompt, parent, ready=True, owner="management", purposes=ManagementVoiceInputs.purposes)
    return value


def edit_basis(link, mark, prompt, parent, basis):
    if type(basis) is not dict or set(basis) != {"version", "input", "prompt_outbox_id", "intent", "words", "draft_id"} or type(basis["version"]) is not int or basis["version"] != 1:
        raise ProductSourceRetired()
    value = management_voice(link, mark, prompt, parent)
    return {**basis, "version": 2, "voice": value} if value is not None else basis


def require_edit_basis(link, mark, prompt, parent, basis):
    original = {key: value for key, value in basis.items() if key != "voice"} | {"version": 1}
    if not same_body(edit_basis(link, mark, prompt, parent, original), basis):
        raise ProductSourceRetired()

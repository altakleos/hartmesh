"""Result presentations retain source order, bounded albums and final controls."""

from copy import deepcopy
from dataclasses import replace

import pytest

from app.telegram_bots.work_requests import WorkRequestRead, seen_basis


def result_source(paths, *, protected=False):
    work = {
        "id": "a" * 32,
        "instance_id": "b" * 32,
        "revision": 8,
        "assignment_revision": 1,
        "objective": "Review the evidence",
        "outcome_id": "c" * 32,
        "outcome": {
            "id": "c" * 32,
            "attempt_id": "d" * 32,
            "assignment_revision": 1,
            "evidence_revision": 1,
            "statement": "All findings are included.",
            "sources": [{"kind": "space_file", "space_id": ("e" if index % 2 else "f") * 32, "path": path, "current_contents": "not_checked"} for index, path in enumerate(paths)],
        },
    }
    request = {
        "id": "1" * 32,
        "instance_id": work["instance_id"],
        "work_id": work["id"],
        "purpose": "review",
        "instance_name": "Assistant",
        "question": "Are these findings acceptable?",
        "reason": "The result needs review.",
        "expected_response": "Accept or request changes.",
        "choices": [],
        "state": "pending",
        "needs_routing": False,
        "can_manage": True,
        "revision": 1,
        "request_revision": 1,
        "assignment_revision": 1,
    }
    return WorkRequestRead(request, work, (), seen_basis(work, request, ()), {}, protected)


def present(source, **kwargs):
    from app.telegram_bots.work_result_files import present_review_files

    return present_review_files(source, locale="en", capacity=300, max_bytes=100, **kwargs)


def test_result_files_keep_cross_space_identity_order_and_card_controls():
    source = result_source(["evidence/a.png", "reports/a.png", "reports/table.csv", "evidence/b.jpg"])
    before = deepcopy(source)
    view = present(source)
    assert source == before
    assert [part["kind"] for part in view.parts] == ["album", "document", "photo", "text"]
    files = [member for part in view.parts[:-1] for member in part["files"]]
    assert [file["source_index"] for file in files] == [0, 1, 2, 3]
    assert [(file["source"]["space_id"], file["source"]["path"]) for file in files] == [(ref["space_id"], ref["path"]) for ref in source.work["outcome"]["sources"]]
    assert all(part["silent"] is True and "reply_to_message_id" not in part and "caption_html" not in part for part in view.parts[:-1])
    assert view.parts[-1]["visible_text"].endswith("Files above:\na.png\na.png\ntable.csv\nb.jpg")
    assert [[button["kind"] for button in row] for row in view.keyboard] == [["accept_open"], ["changes_open"]]


def test_result_albums_reserve_the_whole_unknown_size_batch_before_reads():
    from app.telegram_bots.work_result_files import present_review_files

    source = result_source([f"{index}.png" for index in range(16)])
    limited = present(source)
    assert [len(part["files"]) for part in limited.parts[:-1]] == [3, 3, 3, 3, 3, 1]
    roomy = present_review_files(source, locale="en", capacity=2000, max_bytes=100)
    assert [len(part["files"]) for part in roomy.parts[:-1]] == [10, 6]


def test_concealed_result_has_no_files_names_or_outcome_before_show():
    source = result_source(["private/result.pdf"], protected=True)
    hidden = present(source, concealed=True, outbox_id="2" * 32)
    assert len(hidden.parts) == 1
    assert "result.pdf" not in str(hidden) and "All findings" not in str(hidden)
    assert hidden.keyboard[0][0]["kind"] == "reveal"
    shown = present(source, concealed=False)
    assert all(part["protect_content"] is True for part in shown.parts)
    assert shown.parts[0]["kind"] == "document"


def test_no_files_has_no_trailer_and_multichunk_review_keeps_complete_final_trailer():
    from app.telegram_bots.work_requests import present_request

    empty = result_source([])
    assert present(empty) == present_request(empty, locale="en")
    source = result_source(["report/<result>.pdf"])
    source.work["outcome"]["statement"] = "Evidence & detail. " * 200
    source.request["question"] = "Review each section. " * 80
    view = present(source)
    assert len(view.parts) > 2
    assert view.parts[-1]["visible_text"].endswith("Files above:\n<result>.pdf")
    assert "&lt;result&gt;.pdf" in view.parts[-1]["html"]
    assert "".join(part["visible_text"] for part in view.parts[1:]).count("Evidence & detail.") == 200


def test_redacted_sources_never_recover_private_paths_or_join_other_files():
    source = result_source(["secret/private.pdf", "public/good.png"])
    source.work["outcome"]["sources"][0] = {"kind": "unavailable", "current_contents": "not_checked"}
    view = present(source)
    assert "private.pdf" not in str(view)
    assert view.parts[0]["kind"] == "document"
    assert view.parts[0]["files"] == [{"source_index": 0, "filename": "File", "source": None}]
    assert view.parts[1]["files"][0]["source_index"] == 1


def test_header_classification_keeps_eligible_photos_on_each_side_of_a_document():
    from app.telegram_bots.work_result_files import classify_result_media, result_files

    files = result_files(result_source(["a.png", "b.png", "c.png", "d.png"]).work, locale="en")
    assert classify_result_media(files, [True, False, True, True]) == (("photo", files[:1]), ("document", files[1:2]), ("album", files[2:]))


@pytest.mark.parametrize("change", ["purpose", "outcome", "evidence", "assignment", "path", "count"])
def test_invalid_result_plan_is_refused(change):
    source = result_source(["result.txt"])
    if change == "purpose":
        source = replace(source, request={**source.request, "purpose": "decision"})
    elif change == "outcome":
        source.work["outcome_id"] = "9" * 32
    elif change == "evidence":
        source.work["outcome"]["evidence_revision"] = True
    elif change == "assignment":
        source.work["outcome"]["assignment_revision"] = 2
    elif change == "path":
        source.work["outcome"]["sources"][0]["path"] = "../secret"
    else:
        source.work["outcome"]["sources"] *= 17
    with pytest.raises(ValueError):
        present(source)

"""Read slide text in presentation order, including the speaker's notes."""

from .package import OfficePackage
from .view import MAX_PARAGRAPHS, View


def _slide_text(package, path, view, *, notes=False):
    paragraphs = []
    title = ""
    placeholder = None
    paragraph = None
    in_text = False
    root_seen = False
    for event, tag, data in package.events(path):
        if event == "start":
            if not root_seen:
                if tag != ("p:notes" if notes else "p:sld"):
                    raise ValueError("Unexpected slide part")
                root_seen = True
                if not notes and data.get("show") in {"0", "false"}:
                    return None
            if tag in {"p:sp", "p:graphicFrame"}:
                placeholder = None
            elif tag == "p:ph":
                placeholder = data.get("type", "obj")
            elif tag == "a:p":
                paragraph = ""
            elif tag == "a:t":
                in_text = True
            elif tag == "a:br" and paragraph is not None:
                paragraph = view.text(paragraph + "\n")
        elif event == "text" and in_text and paragraph is not None:
            paragraph = view.text(paragraph + data)
        elif event == "end":
            if tag == "a:t":
                in_text = False
            elif tag == "a:p" and paragraph is not None:
                if paragraph.strip():
                    if not notes and placeholder in {"title", "ctrTitle"} and not title:
                        title = paragraph
                    elif not notes or placeholder == "body":
                        if len(paragraphs) < MAX_PARAGRAPHS:
                            paragraphs.append(paragraph)
                        else:
                            view.notices.add("Additional slide text was left out.")
                paragraph = None
    return title, paragraphs


def project_deck(source: bytes) -> bytes:
    package = OfficePackage(source)
    main = package.main_part()
    rels = package.relationships(main)
    view = View("Slide deck", package.notices)
    view.notices.add("Pictures, charts, embedded objects and slide layout are outside this preview.")
    number = 0
    shown = 0
    root_seen = False
    for event, tag, data in package.events(main):
        if event != "start":
            continue
        if not root_seen:
            if tag != "p:presentation":
                raise ValueError("Expected a presentation")
            root_seen = True
        if tag != "p:sldId":
            continue
        number += 1
        if shown >= 60:
            view.notices.add("Additional slides were left out.")
            break
        kind, path = rels[data["r:id"]]
        if kind != "slide":
            raise ValueError("Invalid slide relationship")
        slide = _slide_text(package, path, view)
        if slide is None:
            view.notices.add("Hidden slides were left out.")
            continue
        title, paragraphs = slide
        for kind, notes_part in package.relationships(path).values():
            if kind == "notesSlide":
                _, notes = _slide_text(package, notes_part, view, notes=True)
                if notes:
                    paragraphs.extend(["Speaker notes", *notes])
                break
        if len(paragraphs) > MAX_PARAGRAPHS:
            view.notices.add("Additional slide text or notes were left out.")
        heading = f"Slide {number}" + (": " + title if title else "")
        if not view.add({"type": "text", "heading": view.text(heading, 256), "paragraphs": paragraphs[:MAX_PARAGRAPHS] or [""]}):
            break
        shown += 1
    return view.finish()

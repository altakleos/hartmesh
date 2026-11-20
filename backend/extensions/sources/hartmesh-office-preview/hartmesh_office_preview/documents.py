"""Produce passive text and tables from the main Word document body."""

from .package import MAX_MEMBERS, OfficePackage
from .view import View, encode

_OMITTED = {"w:del", "w:moveFrom", "w:drawing", "w:pict", "w:object", "w:txbxContent", "?:AlternateContent"}


class DocumentLists:
    def __init__(self, package, main, view):
        self.view = view
        self.previous = None
        self.seen = set()
        self.instances = {}
        self.formats = {}
        self.custom = set()
        for kind, part in package.relationships(main).values():
            if kind == "numbering":
                self._read(package, part)
                break

    def _read(self, package, part):
        abstract = instance = level = None
        for event, tag, data in package.events(part):
            if event == "start":
                value = data.get("w:val")
                if tag == "w:abstractNum":
                    abstract = data["w:abstractNumId"]
                elif tag == "w:num":
                    instance = data["w:numId"]
                elif tag == "w:lvl":
                    level = data["w:ilvl"]
                elif tag == "w:abstractNumId" and instance is not None:
                    self.instances[instance] = value
                elif tag == "w:numFmt" and abstract is not None and level is not None:
                    self.formats[abstract, level] = value
                elif tag == "w:start" and abstract is not None and value != "1":
                    self.custom.add(abstract)
                elif tag == "w:lvlOverride" and instance is not None:
                    self.custom.add(("instance", instance))
                if len(self.instances) + len(self.formats) + len(self.custom) > MAX_MEMBERS:
                    raise ValueError("Too many list definitions")
            elif event == "end":
                if tag == "w:abstractNum":
                    abstract = None
                elif tag == "w:num":
                    instance = None
                elif tag == "w:lvl":
                    level = None

    def add(self, para, style, num_id, level):
        abstract = self.instances.get(num_id)
        fmt = self.formats.get((abstract, level))
        ordered = fmt not in {None, "bullet", "none"} if fmt is not None else style.startswith("listnumber")
        key = (num_id or style, level, ordered)
        if level != "0" or abstract in self.custom or ("instance", num_id) in self.custom or fmt not in {None, "decimal", "bullet"} or (num_id and fmt is None):
            self.view.notices.add("List indentation and numbering may differ from the original.")
        if self.previous == key:
            block = self.view.document["blocks"][-1]
            size = len(encode(para)) + 1
            if len(block["items"]) < 32 and size <= self.view.bytes_left:
                block["items"].append(para)
                self.view.bytes_left -= size
                return True
        if ordered and key in self.seen:
            self.view.notices.add("Continued lists restart their numbering in this preview.")
        self.previous = key
        self.seen.add(key)
        return self.view.add({"type": "list", "items": [para], "ordered": ordered})


def project_document(source: bytes) -> bytes:
    package = OfficePackage(source)
    main = package.main_part()
    view = View("Document", package.notices)
    lists = DocumentLists(package, main, view)
    view.notices.add("Headers, footers, notes, comments, tracked deletions, pictures and embedded objects are outside this preview.")
    body = False
    root_seen = False
    skipped = 0
    in_text = False
    para = None
    style = ""
    num_id = None
    level = "0"
    table = None
    row = None
    cell = None
    for event, tag, data in package.events(main):
        if skipped:
            if event == "start":
                skipped += 1
            elif event == "end":
                skipped -= 1
            continue
        if event == "start":
            if not root_seen:
                if tag != "w:document":
                    raise ValueError("Expected a Word document")
                root_seen = True
            if tag == "w:body":
                body = True
            if not body:
                continue
            if tag in _OMITTED or (tag == "w:tbl" and table is not None):
                skipped = 1
                if tag == "w:tbl":
                    view.notices.add("Nested tables were left out.")
            elif tag == "w:tbl":
                table = []
                lists.previous = None
            elif tag == "w:tr" and table is not None:
                row = []
            elif tag == "w:tc" and row is not None:
                cell = ""
            elif tag == "w:p":
                para, style, num_id, level = "", "", None, "0"
            elif para is not None:
                if tag == "w:pStyle":
                    style = data.get("w:val", "").lower()
                elif tag == "w:numId":
                    num_id = data.get("w:val")
                elif tag == "w:ilvl":
                    level = data.get("w:val", "0")
                elif tag == "w:t":
                    in_text = True
                elif tag in {"w:br", "w:cr", "w:tab"}:
                    para = view.text(para + ("\t" if tag == "w:tab" else "\n"))
        elif event == "text" and in_text and para is not None:
            para = view.text(para + data)
        elif event == "end" and body:
            if tag == "w:t":
                in_text = False
            elif tag == "w:p" and para is not None:
                if cell is not None:
                    cell = view.text((cell + "\n" if cell else "") + para)
                elif para.strip() and num_id != "0" and (num_id is not None or style.startswith("list")):
                    if not lists.add(para, style, num_id, level):
                        break
                elif para.strip():
                    lists.previous = None
                    if style.startswith("heading") or style == "title":
                        block = {"type": "text", "heading": view.text(para, 256), "paragraphs": [""]}
                    else:
                        block = {"type": "text", "paragraphs": [para]}
                    if not view.add(block):
                        break
                else:
                    lists.previous = None
                para = None
            elif tag == "w:tc" and row is not None and cell is not None:
                if len(row) < 12:
                    row.append(cell)
                else:
                    view.notices.add("Only the first 12 table columns are shown.")
                cell = None
            elif tag == "w:tr" and table is not None and row is not None:
                if len(table) < 201:
                    table.append(row)
                else:
                    view.notices.add("Additional table rows were left out.")
                row = None
            elif tag == "w:tbl" and table is not None:
                if table:
                    view.table(table[1:], labels=table[0])
                table = None
            elif tag == "w:body":
                body = False
    return view.finish()

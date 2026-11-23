"""Small valid OOXML examples with deterministic timestamps and no external links."""

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
X = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def relationships(*entries):
    items = "".join(f'<Relationship Id="{key}" Type="{REL}/{kind}" Target="{target}"/>' for key, kind, target in entries)
    return f'<Relationships xmlns="{PKG}">{items}</Relationships>'


def archive(main, main_type, parts, content_types):
    """Package the provided parts with their OPC content types and root relationship."""
    types = {main: main_type, **content_types}
    overrides = "".join(f'<Override PartName="/{name}" ContentType="{kind}"/>' for name, kind in types.items())
    files = {
        "[Content_Types].xml": '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>' + overrides + "</Types>",
        "_rels/.rels": relationships(("rId1", "officeDocument", main)),
        **parts,
    }
    return zip_parts(files)


def zip_parts(files):
    output = BytesIO()
    with ZipFile(output, "w") as target:
        for name, value in files.items():
            target.writestr(ZipInfo(name, date_time=(2024, 1, 1, 0, 0, 0)), value, compress_type=ZIP_DEFLATED)
    return output.getvalue()


def workbook(*, cells=None, shared=None, rows=None):
    types = "application/vnd.openxmlformats-officedocument.spreadsheetml."
    main = "xl/workbook.xml"
    rels = [("rId1", "worksheet", "worksheets/sheet1.xml")]
    parts = {}
    overrides = {"xl/worksheets/sheet1.xml": types + "worksheet+xml"}
    if shared is not None:
        rels.append(("rId2", "sharedStrings", "strings.xml"))
        parts["xl/strings.xml"] = f'<sst xmlns="{X}">{shared}</sst>'
        overrides["xl/strings.xml"] = types + "sharedStrings+xml"
    if cells is None:
        cells = '<c r="A1" t="inlineStr"><is><t>Apples</t></is></c><c r="B1"><v>12</v></c>'
    if rows is None:
        rows = f'<row r="1">{cells}</row>'
    return archive(
        main,
        types + "sheet.main+xml",
        {
            main: f'<workbook xmlns="{X}" xmlns:r="{REL}"><sheets><sheet name="Samples" sheetId="1" r:id="rId1"/></sheets></workbook>',
            "xl/_rels/workbook.xml.rels": relationships(*rels),
            "xl/worksheets/sheet1.xml": f'<worksheet xmlns="{X}"><sheetData>{rows}</sheetData></worksheet>',
            **parts,
        },
        overrides,
    )


def document():
    types = "application/vnd.openxmlformats-officedocument.wordprocessingml."
    main = "word/document.xml"

    def paragraph(text, style=None):
        props = f'<w:pPr><w:pStyle w:val="{style}"/></w:pPr>' if style else ""
        return f"<w:p>{props}<w:r><w:t>{text}</w:t></w:r></w:p>"

    def row(*values):
        return "<w:tr>" + "".join("<w:tc>" + paragraph(value) + "</w:tc>" for value in values) + "</w:tr>"

    body = "".join(
        (
            paragraph("Field notes", "Heading1"),
            paragraph("Opening paragraph."),
            paragraph("Remember the list.", "ListBullet"),
            "<w:tbl><w:tblPr/><w:tblGrid><w:gridCol/><w:gridCol/></w:tblGrid>" + row("Left", "Right") + row("One", "Two") + "</w:tbl>",
            paragraph("Closing paragraph."),
            '<w:sectPr><w:headerReference w:type="default" r:id="rId1"/></w:sectPr>',
        )
    )
    return archive(
        main,
        types + "document.main+xml",
        {
            main: f'<w:document xmlns:w="{W}" xmlns:r="{REL}"><w:body>{body}</w:body></w:document>',
            "word/_rels/document.xml.rels": relationships(("rId1", "header", "header1.xml")),
            "word/header1.xml": f'<w:hdr xmlns:w="{W}">{paragraph("Omitted heading")}</w:hdr>',
        },
        {"word/header1.xml": types + "header+xml"},
    )

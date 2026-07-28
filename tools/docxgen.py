"""
Minimal, dependency-free .docx writer for the IT3388 project deliverables.

Rather than build an OOXML package from nothing, this module clones an existing
document from the repository ("APPENDIX C - PROJECT PROPOSAL.docx") and swaps in a
freshly generated word/document.xml. That way every generated appendix inherits the
same styles.xml, numbering.xml, theme and fonts as the documents the team has already
submitted, so the whole submission set looks consistent.

Supported building blocks: headings, body paragraphs (with **bold** / *italic* inline
markup), bullet and numbered lists, tables with a shaded header row, page breaks,
a cached table of contents, and custom headers/footers with live PAGE fields.

Usage:
    from docxgen import Document
    d = Document()
    d.title("My title", "My subtitle")
    d.heading("Section", 1)
    d.para("Body text with **bold** words.")
    d.save("OUT.docx")
"""

from __future__ import annotations

import os
import re
import shutil
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
DEFAULT_TEMPLATE = os.path.join(REPO, "APPENDIX C \u2013 PROJECT PROPOSAL.docx")

W_NS = (
    'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
    'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
    'xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" '
    'xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math"'
)

# 1.5 line spacing == 360 twentieths of a point, "auto" rule (Appendix E requirement).
LINE_ONE_AND_HALF = 360


def esc(text: str) -> str:
    """XML-escape a string."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _runs_from_markup(text: str, base_rpr: str = "") -> str:
    """Turn '**bold**' / '*italic*' markup into a sequence of <w:r> runs."""
    out = []
    tokens = re.split(r"(\*\*[^*]+\*\*|\*[^*]+\*)", str(text))
    for tok in tokens:
        if not tok:
            continue
        bold = italic = False
        if tok.startswith("**") and tok.endswith("**") and len(tok) > 4:
            bold, tok = True, tok[2:-2]
        elif tok.startswith("*") and tok.endswith("*") and len(tok) > 2:
            italic, tok = True, tok[1:-1]
        props = base_rpr
        if bold:
            props += "<w:b/>"
        if italic:
            props += "<w:i/>"
        rpr = f"<w:rPr>{props}</w:rPr>" if props else ""
        out.append(f'<w:r>{rpr}<w:t xml:space="preserve">{esc(tok)}</w:t></w:r>')
    return "".join(out) or '<w:r><w:t xml:space="preserve"></w:t></w:r>'


class Document:
    """Accumulates block-level XML and writes it into a copy of the template package."""

    def __init__(self, template: str = DEFAULT_TEMPLATE, line_spacing: int | None = LINE_ONE_AND_HALF):
        self.template = template
        self.line_spacing = line_spacing
        self.blocks: list[str] = []
        self.header_lines: list[str] = []
        self.footer_left: str = ""
        self._toc_placeholder_index: int | None = None
        self._toc_entries: list[tuple[str, int, int]] = []

    # ------------------------------------------------------------------ helpers
    def _spacing(self, after: int = 120, before: int = 0) -> str:
        if self.line_spacing:
            return (
                f'<w:spacing w:before="{before}" w:after="{after}" '
                f'w:line="{self.line_spacing}" w:lineRule="auto"/>'
            )
        return f'<w:spacing w:before="{before}" w:after="{after}"/>'

    # ------------------------------------------------------------------- blocks
    def title(self, text: str, subtitle: str | None = None):
        self.blocks.append(
            '<w:p><w:pPr><w:pStyle w:val="Title"/><w:jc w:val="center"/></w:pPr>'
            + _runs_from_markup(text)
            + "</w:p>"
        )
        if subtitle:
            self.blocks.append(
                '<w:p><w:pPr><w:pStyle w:val="Subtitle"/><w:jc w:val="center"/></w:pPr>'
                + _runs_from_markup(subtitle)
                + "</w:p>"
            )
        return self

    def heading(self, text: str, level: int = 1, toc: bool = True):
        if toc:
            self._toc_entries.append((text, level, len(self.blocks)))
        self.blocks.append(
            f'<w:p><w:pPr><w:pStyle w:val="Heading{level}"/>'
            f'{self._spacing(after=120, before=200)}<w:keepNext/></w:pPr>'
            + _runs_from_markup(text)
            + "</w:p>"
        )
        return self

    def para(self, text: str = "", align: str | None = None, size: int | None = None,
             after: int = 120, style: str | None = None):
        ppr = ""
        if style:
            ppr += f'<w:pStyle w:val="{style}"/>'
        ppr += self._spacing(after=after)
        if align:
            ppr += f'<w:jc w:val="{align}"/>'
        base = f'<w:sz w:val="{size}"/><w:szCs w:val="{size}"/>' if size else ""
        self.blocks.append(f"<w:p><w:pPr>{ppr}</w:pPr>" + _runs_from_markup(text, base) + "</w:p>")
        return self

    def bullet(self, text: str, level: int = 1):
        style = "ListBullet" if level == 1 else f"ListBullet{min(level, 3)}"
        self.blocks.append(
            f'<w:p><w:pPr><w:pStyle w:val="{style}"/>{self._spacing(after=60)}</w:pPr>'
            + _runs_from_markup(text)
            + "</w:p>"
        )
        return self

    def bullets(self, items):
        for it in items:
            self.bullet(it)
        return self

    def numbered(self, items, level: int = 1):
        style = "ListNumber" if level == 1 else f"ListNumber{min(level, 3)}"
        for it in items:
            self.blocks.append(
                f'<w:p><w:pPr><w:pStyle w:val="{style}"/>{self._spacing(after=60)}</w:pPr>'
                + _runs_from_markup(it)
                + "</w:p>"
            )
        return self

    def mono(self, lines):
        """Fixed-width block, used for the ER diagram and directory listings."""
        for ln in lines:
            self.blocks.append(
                "<w:p><w:pPr>"
                '<w:spacing w:after="0" w:line="240" w:lineRule="auto"/>'
                "</w:pPr>"
                '<w:r><w:rPr><w:rFonts w:ascii="Consolas" w:hAnsi="Consolas"/>'
                '<w:sz w:val="18"/><w:szCs w:val="18"/></w:rPr>'
                f'<w:t xml:space="preserve">{esc(ln)}</w:t></w:r></w:p>'
            )
        self.para("", after=60)
        return self

    def table(self, rows, widths=None, header=True, font_size=18):
        """rows: list of list of cell strings. widths: list of ints summing to ~9984."""
        if not rows:
            return self
        ncols = max(len(r) for r in rows)
        if widths is None:
            widths = [9984 // ncols] * ncols
        grid = "".join(f'<w:gridCol w:w="{w}"/>' for w in widths)
        xml = [
            "<w:tbl><w:tblPr><w:tblStyle w:val=\"TableGrid\"/>"
            '<w:tblW w:w="0" w:type="auto"/><w:jc w:val="center"/>'
            '<w:tblLook w:val="04A0" w:firstRow="1" w:lastRow="0" w:firstColumn="1" '
            'w:lastColumn="0" w:noHBand="0" w:noVBand="1"/></w:tblPr>'
            f"<w:tblGrid>{grid}</w:tblGrid>"
        ]
        for ri, row in enumerate(rows):
            is_head = header and ri == 0
            cells = []
            for ci in range(ncols):
                text = row[ci] if ci < len(row) else ""
                shade = '<w:shd w:val="clear" w:color="auto" w:fill="14375A"/>' if is_head else ""
                base = f'<w:sz w:val="{font_size}"/><w:szCs w:val="{font_size}"/>'
                if is_head:
                    base += '<w:b/><w:color w:val="FFFFFF"/>'
                cells.append(
                    f'<w:tc><w:tcPr><w:tcW w:w="{widths[ci]}" w:type="dxa"/>{shade}'
                    '<w:vAlign w:val="center"/></w:tcPr>'
                    '<w:p><w:pPr><w:spacing w:before="40" w:after="40" w:line="240" '
                    'w:lineRule="auto"/></w:pPr>'
                    + _runs_from_markup(text, base)
                    + "</w:p></w:tc>"
                )
            trpr = '<w:trPr><w:jc w:val="center"/>' + ("<w:tblHeader/>" if is_head else "") + "</w:trPr>"
            xml.append(f"<w:tr>{trpr}{''.join(cells)}</w:tr>")
        xml.append("</w:tbl>")
        self.blocks.append("".join(xml))
        self.para("", after=60)
        return self

    def page_break(self):
        self.blocks.append(
            '<w:p><w:pPr><w:spacing w:after="0"/></w:pPr>'
            '<w:r><w:br w:type="page"/></w:r></w:p>'
        )
        return self

    def toc(self, depth: int = 2):
        """Reserve a spot for a cached table of contents, filled in at save() time."""
        self._toc_placeholder_index = len(self.blocks)
        self.blocks.append("")  # replaced in save()
        self._toc_depth = depth
        return self

    # ------------------------------------------------------------- header/footer
    def set_header(self, lines):
        self.header_lines = list(lines)
        return self

    def set_footer(self, left_text: str):
        self.footer_left = left_text
        return self

    # ------------------------------------------------------------------- output
    def _build_toc(self) -> str:
        """Cached TOC: real entries with page numbers, wrapped in a live TOC field."""
        depth = getattr(self, "_toc_depth", 2)
        entries = [e for e in self._toc_entries if e[1] <= depth]
        if not entries:
            return ""
        # Estimate the page each heading lands on by counting page breaks before it.
        breaks = [i for i, b in enumerate(self.blocks) if 'w:br w:type="page"' in b]
        paras = []
        for n, (text, level, pos) in enumerate(entries):
            page = 1 + sum(1 for b in breaks if b < pos)
            indent = (level - 1) * 360
            begin = (
                '<w:r><w:fldChar w:fldCharType="begin"/></w:r>'
                '<w:r><w:instrText xml:space="preserve"> TOC \\o "1-'
                f'{depth}" \\h \\z \\u </w:instrText></w:r>'
                '<w:r><w:fldChar w:fldCharType="separate"/></w:r>'
                if n == 0 else ""
            )
            end = '<w:r><w:fldChar w:fldCharType="end"/></w:r>' if n == len(entries) - 1 else ""
            bold = "<w:b/>" if level == 1 else ""
            paras.append(
                "<w:p><w:pPr>"
                f'<w:tabs><w:tab w:val="right" w:leader="dot" w:pos="9639"/></w:tabs>'
                f'<w:spacing w:after="60" w:line="276" w:lineRule="auto"/>'
                f'<w:ind w:left="{indent}"/></w:pPr>'
                + begin
                + f'<w:r><w:rPr>{bold}<w:sz w:val="22"/></w:rPr>'
                f'<w:t xml:space="preserve">{esc(text)}</w:t></w:r>'
                f'<w:r><w:rPr><w:sz w:val="22"/></w:rPr><w:tab/><w:t>{page}</w:t></w:r>'
                + end
                + "</w:p>"
            )
        return "".join(paras)

    @staticmethod
    def _styleref_runs(rpr_props: str) -> str:
        """A STYLEREF field: Word substitutes the current Heading 1 text on every page, which is
        how the 'chapter name in the header' requirement is met without one section per chapter."""
        rpr = f"<w:rPr>{rpr_props}</w:rPr>" if rpr_props else ""
        return (
            f'<w:r>{rpr}<w:fldChar w:fldCharType="begin"/></w:r>'
            f'<w:r>{rpr}<w:instrText xml:space="preserve"> STYLEREF "Heading 1" '
            '\\* MERGEFORMAT </w:instrText></w:r>'
            f'<w:r>{rpr}<w:fldChar w:fldCharType="separate"/></w:r>'
            f"<w:r>{rpr}<w:t>Chapter</w:t></w:r>"
            f'<w:r>{rpr}<w:fldChar w:fldCharType="end"/></w:r>'
        )

    def _header_xml(self) -> str:
        if not self.header_lines:
            return None
        small = '<w:sz w:val="16"/><w:szCs w:val="16"/><w:color w:val="44546A"/>'
        ps = []
        for i, ln in enumerate(self.header_lines):
            # "{CHAPTER}" expands to a live field showing the current Heading 1.
            runs = []
            for j, part in enumerate(str(ln).split("{CHAPTER}")):
                if j:
                    runs.append(self._styleref_runs(small))
                if part:
                    runs.append(_runs_from_markup(part, small))
            ps.append(
                '<w:p><w:pPr><w:pStyle w:val="Header"/>'
                '<w:pBdr>' + ('<w:bottom w:val="single" w:sz="4" w:space="1" w:color="14375A"/>'
                              if i == len(self.header_lines) - 1 else "") + "</w:pBdr>"
                '<w:spacing w:after="0"/></w:pPr>'
                + "".join(runs)
                + "</w:p>"
            )
        body = "".join(ps)
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            f"<w:hdr {W_NS}>{body}</w:hdr>"
        )

    def _footer_xml(self) -> str:
        left = _runs_from_markup(
            self.footer_left, '<w:sz w:val="16"/><w:szCs w:val="16"/><w:color w:val="44546A"/>'
        )
        small = '<w:rPr><w:sz w:val="16"/><w:szCs w:val="16"/><w:color w:val="44546A"/></w:rPr>'
        body = (
            '<w:p><w:pPr><w:pStyle w:val="Footer"/>'
            '<w:tabs><w:tab w:val="right" w:pos="9639"/></w:tabs>'
            '<w:spacing w:after="0"/></w:pPr>'
            + left
            + f"<w:r>{small}<w:tab/><w:t xml:space=\"preserve\">Page </w:t></w:r>"
            + f'<w:r>{small}<w:fldChar w:fldCharType="begin"/></w:r>'
            + f'<w:r>{small}<w:instrText xml:space="preserve"> PAGE </w:instrText></w:r>'
            + f'<w:r>{small}<w:fldChar w:fldCharType="separate"/></w:r>'
            + f"<w:r>{small}<w:t>1</w:t></w:r>"
            + f'<w:r>{small}<w:fldChar w:fldCharType="end"/></w:r>'
            + f"<w:r>{small}<w:t xml:space=\"preserve\"> of </w:t></w:r>"
            + f'<w:r>{small}<w:fldChar w:fldCharType="begin"/></w:r>'
            + f'<w:r>{small}<w:instrText xml:space="preserve"> NUMPAGES </w:instrText></w:r>'
            + f'<w:r>{small}<w:fldChar w:fldCharType="separate"/></w:r>'
            + f"<w:r>{small}<w:t>1</w:t></w:r>"
            + f'<w:r>{small}<w:fldChar w:fldCharType="end"/></w:r>'
            "</w:p>"
        )
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            f"<w:ftr {W_NS}>{body}</w:ftr>"
        )

    def save(self, path: str):
        if self._toc_placeholder_index is not None:
            self.blocks[self._toc_placeholder_index] = self._build_toc()

        sect = (
            "<w:sectPr>"
            '<w:headerReference w:type="default" r:id="rId8"/>'
            '<w:footerReference w:type="default" r:id="rId9"/>'
            '<w:pgSz w:w="12240" w:h="15840"/>'
            '<w:pgMar w:top="1080" w:right="1123" w:bottom="1080" w:left="1123" '
            'w:header="576" w:footer="576" w:gutter="0"/>'
            '<w:cols w:space="720"/><w:docGrid w:linePitch="360"/>'
            "</w:sectPr>"
        )
        document = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            f"<w:document {W_NS}><w:body>"
            + "".join(self.blocks)
            + sect
            + "</w:body></w:document>"
        )

        header_xml = self._header_xml()
        footer_xml = self._footer_xml()

        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        src = zipfile.ZipFile(self.template)
        try:
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as out:
                for item in src.infolist():
                    name = item.filename
                    if name == "word/document.xml":
                        out.writestr(name, document)
                    elif name == "word/header1.xml" and header_xml:
                        out.writestr(name, header_xml)
                    elif name == "word/footer1.xml":
                        out.writestr(name, footer_xml)
                    elif name == "word/settings.xml":
                        data = src.read(name).decode("utf-8")
                        if "<w:updateFields" not in data:
                            data = data.replace(
                                "<w:footnotePr>",
                                '<w:updateFields w:val="true"/><w:footnotePr>',
                                1,
                            )
                        out.writestr(name, data)
                    elif name == "docProps/core.xml":
                        data = src.read(name).decode("utf-8")
                        base = os.path.basename(path).rsplit(".", 1)[0]
                        data = re.sub(
                            r"<dc:title>.*?</dc:title>", f"<dc:title>{esc(base)}</dc:title>", data
                        )
                        out.writestr(name, data)
                    else:
                        out.writestr(item, src.read(name))
        finally:
            src.close()
        return path


def check(path: str) -> str:
    """Light structural validation of a generated package."""
    z = zipfile.ZipFile(path)
    bad = z.testzip()
    if bad:
        raise RuntimeError(f"corrupt member {bad} in {path}")
    from xml.etree import ElementTree as ET

    for name in z.namelist():
        if name.endswith(".xml") or name.endswith(".rels"):
            ET.fromstring(z.read(name))
    size = os.path.getsize(path)
    return f"OK  {os.path.basename(path)}  ({size/1024:.0f} KB, {len(z.namelist())} parts)"

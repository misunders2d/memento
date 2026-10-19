"""Text and pictures out of files: PDF pages, document text, chunks, images, OCR.

Ports of DigUp `Extractors.swift` (`PDFExtractor`, `DocExtractor`, `ImageExtractor`, `excerpt`),
`SearchText.hasWords` and `Rules.screenshotPrefixes`. Office/OpenDocument files are read with zipfile + XML
(DigUp used the Cocoa text system), OCR is the Tesseract CLI (DigUp used Apple Vision).
Stdlib + pypdfium2 + Pillow (+ pillow-heif when installed) only.
"""
from __future__ import annotations

import io
import json
import logging
import re
import shutil
import subprocess
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import pypdfium2 as pdfium
from PIL import Image, ImageOps

log = logging.getLogger(__name__)

try:  # HEIC/HEIF photos (phones)
    import pillow_heif

    pillow_heif.register_heif_opener()
except ImportError:  # pragma: no cover - optional
    pass

SCREENSHOT_PREFIXES = ("Screenshot", "Screen Shot", "CleanShot", "SCR-")
RAW_EXTENSIONS = {"cr2", "cr3", "crw", "nef", "nrw", "arw", "srf", "sr2", "dng", "raf", "orf", "rw2", "pef", "srw",
                  "raw", "rwl", "3fr", "erf", "kdc", "mrw", "x3f"}


class SkipFile(Exception):
    """The file can't be indexed and won't become indexable by retrying (too small, unreadable, locked)."""


# ------------------------------------------------------------- small helpers (SearchText.swift, Rules.swift)
def is_screenshot(name: str) -> bool:
    """Screenshot by name (DigUp `Rules.screenshotPrefixes`; the macOS xattr has no Drive equivalent)."""
    return name.startswith(SCREENSHOT_PREFIXES)


def has_words(text: str) -> bool:
    """Whether text says something in words: at least a fifth of what isn't space is letters (marks count, such as
    Bengali vowel signs). A data dump or a table of numbers isn't, and gets keyword search only."""
    letters = others = 0
    for ch in text:
        if ch.isspace():
            continue
        if unicodedata.category(ch)[0] in "LM":
            letters += 1
        else:
            others += 1
    return letters > 0 and letters * 4 >= others


def excerpt(text: str, limit: int = 240) -> str:
    """One line, at most `limit` characters (plus "…" when cut): for result lists."""
    line = re.sub(r"\s+", " ", text).strip()
    return line[:limit] + "…" if len(line) > limit else line


# ------------------------------------------------------------- PDF (PDFExtractor)
@dataclass(frozen=True)
class Page:
    number: int                     # 1-based
    text: str                       # may be empty for scans
    image: Image.Image | None       # set when the page had too little text and was rendered


class PdfRead(NamedTuple):
    pages: list[Page]               # the first `max_pages` pages
    total: int                      # pages in the file
    title: str | None               # from the document metadata


def pdf_pages(path, min_text: int = 200, render_px: int = 1400, max_pages: int = 5000) -> PdfRead:
    """PdfRead(pages, total, title), as DigUp's `PDFExtractor.pages`. Pages with ≥ `min_text` characters of text
    become text; scans and slides are rendered into a square of `render_px` (aspect kept) and keep whatever short
    text they have. At most `max_pages` pages. Rendered images are JPEG-backed (decoded on first use).
    Raises SkipFile for locked or unreadable PDFs."""
    try:
        pdf = pdfium.PdfDocument(str(path))
    except pdfium.PdfiumError as e:
        raise SkipFile("password-protected" if "password" in str(e).lower() else f"unreadable: {e}") from e
    try:
        pages: list[Page] = []
        for index in range(min(len(pdf), max_pages)):
            page = pdf[index]
            try:
                textpage = page.get_textpage()
                try:
                    text = textpage.get_text_bounded().replace("\r\n", "\n").replace("\r", "\n").strip()
                finally:
                    textpage.close()
                if len(text) >= min_text:
                    pages.append(Page(index + 1, text, None))
                    continue
                try:
                    width, height = page.get_size()
                    bitmap = page.render(scale=render_px / max(width, height, 1.0))
                    image = _jpeg_backed(bitmap.to_pil())
                except pdfium.PdfiumError as e:  # DigUp: a page that won't render is left out
                    log.warning("pdf page %d not rendered: %s", index + 1, e)
                    continue
                pages.append(Page(index + 1, text, image))
            finally:
                page.close()
        title = (pdf.get_metadata_dict(skip_empty=True).get("Title") or "").strip() or None
        return PdfRead(pages, len(pdf), title)
    finally:
        pdf.close()


def _jpeg_backed(img: Image.Image) -> Image.Image:
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=90)
    buf.seek(0)
    return Image.open(buf)


# ------------------------------------------------------------- documents (DocExtractor)
# Text read per document at most: ~5,000 passages of 1,800 characters (DigUp's maxChunks); the rest is never indexed,
# so reading it would only cost memory (a big or crafted file must not exhaust the indexer).
MAX_TEXT_CHARS = 9_000_000
# Decompressed bytes read from one zip member (docx/xlsx/odt XML) at most: bounds the XML parser's memory too, so a
# zip bomb can't exhaust the indexer. Real documents' XML is far smaller.
MAX_MEMBER_BYTES = MAX_TEXT_CHARS * 4


class _Bounded(io.RawIOBase):
    """A zip member read through a byte budget: past it, the document is skipped (SkipFile)."""

    def __init__(self, fp, limit: int) -> None:
        self.fp, self.left = fp, limit

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        data = self.fp.read(min(len(b), self.left + 1))
        if len(data) > self.left:
            raise SkipFile("document content too large")
        self.left -= len(data)
        b[:len(data)] = data
        return len(data)


def _open_member(z: zipfile.ZipFile, name: str):
    return io.BufferedReader(_Bounded(z.open(name), MAX_MEMBER_BYTES))


def doc_text(path, ext: str) -> str:
    """Plain text of a document. `ext` is the file extension (with or without the dot, any case).
    txt/md/code/csv/tsv as text; html stripped of tags; docx/pptx/xlsx/odt/odp/ods via zipfile + XML (paragraph,
    slide and row breaks kept as newlines, cells tab-separated); rtf by a minimal control-word stripper; ipynb as
    its cell sources. Raises SkipFile when the file isn't what its extension says."""
    ext = ext.lower().lstrip(".")
    path = Path(path)
    try:
        if ext in ("html", "htm"):
            return plain_text_from_html(_read_text(path))
        if ext == "rtf":
            return _rtf_text(_read_bytes(path).decode("latin-1"))
        if ext == "ipynb":
            return _ipynb_text(path)
        if ext == "docx":
            return _zip_xml(path, ["word/document.xml"], {"p"}, _ooxml_text)
        if ext == "pptx":
            return _zip_xml(path, _numbered(path, r"ppt/slides/slide(\d+)\.xml"), {"p"}, _ooxml_text)
        if ext == "xlsx":
            return _xlsx_text(path)
        if ext in ("odt", "odp", "ods"):
            return _zip_xml(path, ["content.xml"], {"p", "h", "table-row"}, _odf_block, seps={"page", "table"})
    except (zipfile.BadZipFile, KeyError, ET.ParseError, json.JSONDecodeError, UnicodeDecodeError) as e:
        raise SkipFile(f"unreadable {ext}: {e}") from e
    return _read_text(path)


def _read_bytes(path: Path) -> bytes:
    """At most MAX_TEXT_CHARS * 4 bytes of a file (txt, md, code, html, rtf, ipynb)."""
    with open(path, "rb") as f:
        return f.read(MAX_TEXT_CHARS * 4)


TEXT_DECODING = 2  # _read_text's version: a change re-reads uploaded plain-text files (indexer.content_key)
_NOT_PLAIN_TEXT = {"docx", "pptx", "xlsx", "odt", "odp", "ods", "rtf"}


def read_as_plain_text(ext: str) -> bool:
    """doc_text reads a file with this extension through _read_text."""
    return ext.lower().lstrip(".") not in _NOT_PLAIN_TEXT


def _read_text(path: Path) -> str:
    """UTF-8 (or UTF-16 with a BOM), else Windows Cyrillic or Latin-1 (_single_byte): DigUp's
    `String(contentsOf:usedEncoding:)` fallback, plus Cyrillic. Reads at most MAX_TEXT_CHARS * 4 bytes; a character
    cut off by that limit is dropped. A NUL byte in the first 8 KB means binary content under a text name or MIME
    type (Drive calls some videos text/*): SkipFile, rather than embedding the bytes as text."""
    data = _read_bytes(path)
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16")
    if b"\x00" in data[:8192]:
        raise SkipFile("binary content, not text")
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        if e.reason == "unexpected end of data" and len(data) == MAX_TEXT_CHARS * 4:   # cut mid-character
            return data[:e.start].decode("utf-8-sig")
    return _single_byte(data)


def _single_byte(data: bytes) -> str:
    """Windows Cyrillic (cp1251) when it reads as more Cyrillic letters than Latin ones, else Latin-1."""
    try:
        text = data.decode("cp1251")
    except UnicodeDecodeError:   # 0x98 is unassigned in cp1251
        return data.decode("latin-1")
    cyrillic = sum("Ѐ" <= ch <= "ӿ" for ch in text)
    latin = sum(ch.isascii() and ch.isalpha() for ch in text)
    return text if cyrillic > latin else data.decode("latin-1")


_HTML_DROP = [re.compile(p) for p in (r"(?is)<script\b.*?</script\s*>", r"(?is)<style\b.*?</style\s*>",
                                       r"(?s)<!--.*?-->", r"(?is)<head\b.*?</head\s*>")]
_HTML_BREAK = re.compile(r"(?i)<(br|/p|/div|/li|/h[1-6]|/tr|/section|/article)\b[^>]*>")
_HTML_ENTITIES = [("&nbsp;", " "), ("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'), ("&#39;", "'"), ("&apos;", "'"),
                  ("&mdash;", "—"), ("&ndash;", "–"), ("&hellip;", "…"), ("&amp;", "&")]
_NUMERIC_ENTITY = re.compile(r"&#(x[0-9a-fA-F]+|[0-9]+);")


def plain_text_from_html(html: str) -> str:
    """Good enough for search: drops scripts, styles and comments, turns block ends into line breaks, strips the
    remaining tags, and decodes common entities. Port of DigUp `DocExtractor.plainText(fromHTML:)`."""
    text = html
    for pattern in _HTML_DROP:
        text = pattern.sub(" ", text)
    text = _HTML_BREAK.sub("\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    for entity, character in _HTML_ENTITIES:  # &amp; last, so "&amp;lt;" stays "&lt;"
        text = text.replace(entity, character)
    text = _NUMERIC_ENTITY.sub(_numeric_entity, text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()


def _numeric_entity(m: re.Match) -> str:
    code = m.group(1)
    value = int(code[1:], 16) if code[0] in "xX" else int(code)
    return chr(value) if value <= 0x10FFFF and not 0xD800 <= value <= 0xDFFF else " "


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _numbered(path: Path, pattern: str) -> list[str]:
    """Zip members matching `pattern` (one number group), in numeric order: slide1, slide2, ..., slide10."""
    rx = re.compile(pattern)
    with zipfile.ZipFile(path) as z:
        found = [(int(m.group(1)), n) for n in z.namelist() if (m := rx.fullmatch(n))]
    return [n for _, n in sorted(found)]


def _xml_blocks(fp, blocks: set[str], text_of, seps: set[str] = frozenset()):
    """The text of each outermost `blocks` element in document order, and "" at the end of each `seps` element
    outside blocks (a blank line between slides or sheets). Parsed incrementally, clearing what's been read, so a
    big sheet never sits in memory whole."""
    stack: list[ET.Element] = []
    depth = 0
    for event, el in ET.iterparse(fp, events=("start", "end")):
        tag = _local(el.tag)
        if event == "start":
            stack.append(el)
            depth += tag in blocks
            continue
        stack.pop()
        if tag in blocks:
            depth -= 1
            if depth == 0:
                yield text_of(el)
                if stack:
                    stack[-1].clear()  # everything in the parent so far is read
        elif tag in seps and depth == 0:
            yield ""


def _zip_xml(path: Path, members: list[str], blocks: set[str], text_of, seps: set[str] = frozenset()) -> str:
    parts: list[str] = []
    total = 0
    with zipfile.ZipFile(path) as z:
        for i, name in enumerate(members):
            if i:
                parts.append("")  # blank line between slides
            with _open_member(z, name) as fp:
                for block in _xml_blocks(fp, blocks, text_of, seps):
                    parts.append(block)
                    total += len(block) + 1
                    if total > MAX_TEXT_CHARS:
                        break
            if total > MAX_TEXT_CHARS:
                break
    return re.sub(r"\n{3,}", "\n\n", "\n".join(parts)).strip()  # empty rows and paragraphs: one blank line


_OOXML_SKIP = {"pPr", "rPr", "Fallback", "instrText", "delText"}


def _ooxml_text(el: ET.Element) -> str:
    """A WordprocessingML (w:p) or DrawingML (a:p) paragraph: runs' text, tabs and breaks; nested paragraphs
    (text boxes) on their own lines."""
    out: list[str] = []

    def walk(node: ET.Element) -> None:
        for ch in node:
            tag = _local(ch.tag)
            if tag == "t":
                out.append(ch.text or "")
            elif tag == "tab":
                out.append("\t")
            elif tag in ("br", "cr"):
                out.append("\n")
            elif tag not in _OOXML_SKIP:
                walk(ch)
                if tag == "p":
                    out.append("\n")

    walk(el)
    return "".join(out).rstrip("\n")


def _repeat(value) -> int:
    """text:s repeat count from the file: bounded (a crafted count must not allocate gigabytes)."""
    try:
        return min(max(int(value), 0), 64) if value is not None else 1
    except ValueError:
        return 1


def _odf_inline(el: ET.Element) -> str:
    """Text of an OpenDocument element: text, spaces (text:s), tabs and line breaks; nested paragraphs on their
    own lines; comments left out."""
    out = [el.text or ""]
    for ch in el:
        tag = _local(ch.tag)
        if tag == "s":
            out.append(" " * _repeat(ch.get("{urn:oasis:names:tc:opendocument:xmlns:text:1.0}c")))
        elif tag == "tab":
            out.append("\t")
        elif tag == "line-break":
            out.append("\n")
        elif tag != "annotation":
            out.append(_odf_inline(ch))
            if tag in ("p", "h"):
                out.append("\n")
        out.append(ch.tail or "")
    return "".join(out)


def _odf_block(el: ET.Element) -> str:
    if _local(el.tag) == "table-row":  # one line, non-empty cells tab-separated
        cells = (_odf_inline(c).strip().replace("\n", " ") for c in el if _local(c.tag) == "table-cell")
        return "\t".join(c for c in cells if c)
    return _odf_inline(el).rstrip("\n")


def _si_text(el: ET.Element) -> str:
    """A shared or inline string: plain `t`, or rich-text runs `r/t` (phonetic `rPh` left out)."""
    out: list[str] = []
    for ch in el:
        tag = _local(ch.tag)
        if tag == "t":
            out.append(ch.text or "")
        elif tag == "r":
            out.extend(t.text or "" for t in ch if _local(t.tag) == "t")
    return "".join(out)


def _xlsx_text(path: Path) -> str:
    """Every sheet's cell values (shared strings resolved), one row per line, non-empty cells tab-separated,
    sheets separated by a blank line."""
    with zipfile.ZipFile(path) as z:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in z.namelist():
            with _open_member(z, "xl/sharedStrings.xml") as fp:
                size = 0
                for text in _xml_blocks(fp, {"si"}, _si_text):
                    shared.append(text)
                    size += len(text)
                    if size > MAX_TEXT_CHARS:
                        break

        def row(el: ET.Element) -> str:
            values, size = [], 0
            for c in el:
                if size > MAX_TEXT_CHARS:  # one row repeating a huge shared string must not build gigabytes
                    break
                if _local(c.tag) != "c":
                    continue
                kind = c.get("t")
                if kind == "inlineStr":
                    value = "".join(_si_text(i) for i in c if _local(i.tag) == "is")
                else:
                    v = next((x.text for x in c if _local(x.tag) == "v"), None) or ""
                    value = shared[int(v)] if kind == "s" and v.isdigit() and int(v) < len(shared) else v
                if value.strip():
                    values.append(value.strip().replace("\n", " "))
                    size += len(values[-1]) + 1
            return "\t".join(values)

        parts: list[str] = []
        total = 0
        for i, name in enumerate(_numbered(path, r"xl/worksheets/sheet(\d+)\.xml")):
            if i:
                parts.append("")
            with _open_member(z, name) as fp:
                for line in _xml_blocks(fp, {"row"}, row):
                    if line:
                        parts.append(line)
                        total += len(line) + 1
                        if total > MAX_TEXT_CHARS:
                            break
            if total > MAX_TEXT_CHARS:
                break
    return "\n".join(parts).strip()


def _ipynb_text(path: Path) -> str:
    """Cell sources only (markdown and code), cells separated by a blank line; outputs left out."""
    nb = json.loads(_read_text(path))
    sources = []
    for cell in nb.get("cells", []):
        src = cell.get("source", "")
        sources.append("".join(src) if isinstance(src, list) else str(src))
    return "\n\n".join(s.strip("\n") for s in sources if s.strip())


# RTF: control words, \'hh escapes, control symbols, braces, raw line breaks, plain text.
_RTF_TOKEN = re.compile(r"\\([a-zA-Z]+)(-?\d+)? ?|\\'([0-9a-fA-F]{2})|\\([^a-zA-Z])|([{}])|[\r\n]+|([^\\{}\r\n]+)")
_RTF_DESTINATIONS = {"fonttbl", "colortbl", "stylesheet", "info", "pict", "object", "themedata", "colorschememapping",
                     "latentstyles", "datastore", "xmlnstbl", "listtable", "listoverridetable", "rsidtbl", "generator",
                     "filetbl", "revtbl", "header", "headerl", "headerr", "headerf", "footer", "footerl", "footerr",
                     "footerf", "fldinst"}
_RTF_WORDS = {"par": "\n", "line": "\n", "sect": "\n", "page": "\n", "row": "\n", "tab": "\t", "cell": "\t",
              "emdash": "—", "endash": "–", "bullet": "•", "lquote": "‘", "rquote": "’", "ldblquote": "“",
              "rdblquote": "”", "emspace": " ", "enspace": " ", "qmspace": " "}
_RTF_SYMBOLS = {"\\": "\\", "{": "{", "}": "}", "~": " ", "-": "", "_": "-", "\n": "\n", "\r": "\n"}


def _rtf_text(raw: str) -> str:
    """Minimal RTF to text: keeps text and \\'hh / \\uN characters, turns \\par, \\line, \\tab, \\cell, \\row into
    breaks, and drops control words, fonts, colours, styles, pictures, headers and \\* destinations."""
    out: list[str] = []
    stack: list[tuple[bool, int]] = []
    skip, uc, pending, codepage = False, 1, 0, "cp1252"  # pending: fallback characters to drop after \uN
    for m in _RTF_TOKEN.finditer(raw):
        word, arg, hexcode, symbol, brace, text = m.groups()
        if brace == "{":
            stack.append((skip, uc))
        elif brace == "}":
            skip, uc = stack.pop() if stack else (False, 1)
            pending = 0
        elif word is not None:
            pending = 0
            if word in _RTF_DESTINATIONS:
                skip = True
            elif word == "ansicpg" and arg:
                codepage = f"cp{arg}"
            elif word == "uc" and arg:
                uc = int(arg)
            elif word == "u" and arg:
                if not skip:
                    out.append(chr(int(arg) % 65536))
                pending = uc
            elif not skip and word in _RTF_WORDS:
                out.append(_RTF_WORDS[word])
        elif hexcode is not None:
            if pending:
                pending -= 1
            elif not skip:
                try:
                    out.append(bytes([int(hexcode, 16)]).decode(codepage, errors="replace"))
                except LookupError:
                    out.append(bytes([int(hexcode, 16)]).decode("cp1252", errors="replace"))
        elif symbol is not None:
            if symbol == "*":
                skip = True
            elif pending:
                pending -= 1
            elif not skip:
                out.append(_RTF_SYMBOLS.get(symbol, ""))
        elif text is not None:
            if pending:
                cut = min(pending, len(text))
                text, pending = text[cut:], pending - cut
            if not skip:
                out.append(text)
    # \uN gives UTF-16 units: join surrogate pairs, replace lone ones (they can't be stored as UTF-8).
    text = "".join(out).encode("utf-16-le", "surrogatepass").decode("utf-16-le", "replace")
    return re.sub(r"\n{3,}", "\n\n", re.sub(r"[ \t]+\n", "\n", text)).strip()


# ------------------------------------------------------------- images (ImageExtractor, OCR)
def prepare_image(path, max_px: int, min_side: int = 256) -> tuple[Image.Image, int, int]:
    """(RGB copy that fits in `max_px`, width, height of the original). JPEGs decode straight at reduced size, EXIF
    orientation is applied, transparency goes onto white. Raises SkipFile for images under `min_side` px on the
    long side, RAW files and anything Pillow (+ pillow-heif) can't read."""
    if Path(path).suffix.lower().lstrip(".") in RAW_EXTENSIONS:
        raise SkipFile("RAW image")
    try:
        with Image.open(path) as im:
            width, height = im.size
            if max(width, height) < min_side:
                raise SkipFile(f"small image {width}×{height}")
            im.draft("RGB", (max_px, max_px))  # JPEG: decode at 1/2, 1/4, 1/8 when that still covers max_px
            img = ImageOps.exif_transpose(im)
            if "A" in img.getbands() or "transparency" in img.info:
                rgba = img.convert("RGBA")
                img = Image.new("RGB", rgba.size, (255, 255, 255))
                img.paste(rgba, mask=rgba.getchannel("A"))
            elif img.mode != "RGB":  # before resizing: palette images would resize with nearest neighbour
                img = img.convert("RGB")
            img.thumbnail((max_px, max_px), Image.Resampling.LANCZOS)
    except SkipFile:
        raise
    except (OSError, ValueError, Image.DecompressionBombError) as e:  # UnidentifiedImageError is an OSError
        raise SkipFile(f"unreadable image: {e}") from e
    return img, width, height


_ocr_missing_logged = False


def ocr(image: Image.Image, langs: str = "eng", timeout: float = 60) -> str:
    """Text in the image via the Tesseract CLI (`tesseract stdin stdout -l <langs>`, PNG on stdin), one line per
    recognised line. "" when Tesseract is missing (logged once) or fails. DigUp reads screenshots at full
    resolution: small UI text is what they're full of, so pass the original image, not the model's copy."""
    global _ocr_missing_logged
    exe = shutil.which("tesseract")
    if exe is None:
        if not _ocr_missing_logged:
            log.warning("tesseract not found: OCR disabled (screenshots get no OCR text)")
            _ocr_missing_logged = True
        return ""
    if image.mode not in ("1", "L", "LA", "P", "RGB", "RGBA"):
        image = image.convert("RGB")
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    try:
        run = subprocess.run([exe, "stdin", "stdout", "-l", langs], input=buf.getvalue(), capture_output=True,
                             timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        log.warning("tesseract timed out after %s s", timeout)
        return ""
    if run.returncode != 0:
        log.warning("tesseract failed (%d): %s", run.returncode, run.stderr.decode(errors="replace").strip()[-300:])
        return ""
    text = run.stdout.decode("utf-8", errors="replace")
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())

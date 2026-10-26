//! Existing document projections, preserving Unicode characters and line breaks.
use crate::{Error, Result};
use encoding_rs::{Encoding, WINDOWS_1251, WINDOWS_1252};
use quick_xml::{Reader, events::Event};
use regex::{Captures, Regex};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    sync::LazyLock,
};
use zip::ZipArchive;

pub const MAX_TEXT_CHARS: usize = 9_000_000;
pub const MAX_MEMBER_BYTES: usize = MAX_TEXT_CHARS * 4;
fn re(pattern: &str) -> Regex {
    Regex::new(pattern).expect("constant regular expression")
}
fn space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}
fn trim(s: &str) -> &str {
    s.trim_matches(space)
}
fn latin1(data: &[u8]) -> String {
    data.iter().map(|b| char::from(*b)).collect()
}
fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    File::open(path)?
        .take(MAX_MEMBER_BYTES as u64)
        .read_to_end(&mut out)?;
    Ok(out)
}
pub fn read_text(path: &Path) -> Result<String> {
    decode_text(&read_bytes(path)?)
}
pub fn decode_text(data: &[u8]) -> Result<String> {
    if data.starts_with(&[255, 254]) || data.starts_with(&[254, 255]) {
        if data.len() % 2 != 0 {
            return Err(Error::Decode("incomplete UTF-16"));
        }
        let le = data[0] == 255;
        return char::decode_utf16(data[2..].chunks_exact(2).map(|b| {
            if le {
                u16::from_le_bytes([b[0], b[1]])
            } else {
                u16::from_be_bytes([b[0], b[1]])
            }
        }))
        .collect::<std::result::Result<String, _>>()
        .map_err(|_| Error::Decode("invalid UTF-16"));
    }
    if data.iter().take(8192).any(|b| *b == 0) {
        return Err(Error::Decode("binary content, not text"));
    }
    let utf8 = data.strip_prefix(&[239, 187, 191]).unwrap_or(data);
    match std::str::from_utf8(utf8) {
        Ok(s) => return Ok(s.into()),
        Err(e) if e.error_len().is_none() && data.len() == MAX_MEMBER_BYTES => {
            return Ok(std::str::from_utf8(&utf8[..e.valid_up_to()])
                .unwrap()
                .into());
        }
        _ => (),
    }
    if data.contains(&0x98) {
        return Ok(latin1(data));
    }
    let (cyrillic, _, bad) = WINDOWS_1251.decode(data);
    if bad {
        return Ok(latin1(data));
    }
    let cyr = cyrillic
        .chars()
        .filter(|c| ('\u{400}'..='\u{4ff}').contains(c))
        .count();
    let latin = cyrillic.chars().filter(|c| c.is_ascii_alphabetic()).count();
    Ok(if cyr > latin {
        cyrillic.into_owned()
    } else {
        latin1(data)
    })
}
pub fn document_text(path: &Path, extension: &str) -> Result<String> {
    let ext = extension.trim_start_matches('.').to_ascii_lowercase();
    match ext.as_str() {
        "html" | "htm" => Ok(plain_text_from_html(&read_text(path)?)),
        "rtf" => Ok(rtf_text(&latin1(&read_bytes(path)?))),
        "ipynb" => {
            let nb: Value = serde_json::from_str(&read_text(path)?)?;
            let mut parts = Vec::new();
            for cell in nb["cells"].as_array().into_iter().flatten() {
                let src = &cell["source"];
                let text = if let Some(a) = src.as_array() {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or(""))
                        .collect::<String>()
                } else {
                    src.as_str().unwrap_or("").into()
                };
                if !trim(&text).is_empty() {
                    parts.push(text.trim_matches('\n').to_owned());
                }
            }
            Ok(parts.join("\n\n"))
        }
        "docx" => zip_text(path, vec!["word/document.xml".into()], &["p"], &[], ooxml),
        "pptx" => zip_text(
            path,
            numbered(path, "ppt/slides/slide", ".xml")?,
            &["p"],
            &[],
            ooxml,
        ),
        "xlsx" => xlsx(path),
        "odt" | "odp" | "ods" => zip_text(
            path,
            vec!["content.xml".into()],
            &["p", "h", "table-row"],
            &["page", "table"],
            odf_block,
        ),
        _ => read_text(path),
    }
}
pub fn plain_text_from_html(html: &str) -> String {
    static DROP: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        [
            r"(?is)<script\b.*?</script\s*>",
            r"(?is)<style\b.*?</style\s*>",
            r"(?s)<!--.*?-->",
            r"(?is)<head\b.*?</head\s*>",
        ]
        .into_iter()
        .map(re)
        .collect()
    });
    static BREAK: LazyLock<Regex> =
        LazyLock::new(|| re(r"(?i)<(br|/p|/div|/li|/h[1-6]|/tr|/section|/article)\b[^>]*>"));
    static TAG: LazyLock<Regex> = LazyLock::new(|| re(r"<[^>]+>"));
    static NUM: LazyLock<Regex> = LazyLock::new(|| re(r"&#(x[0-9a-fA-F]+|[0-9]+);"));
    static HORIZONTAL: LazyLock<Regex> = LazyLock::new(|| re(r"[ \t]+"));
    static LINE: LazyLock<Regex> =
        LazyLock::new(|| re(r"[\p{White_Space}\x1c-\x1f]*\n[\p{White_Space}\x1c-\x1f]*"));
    let mut text = html.to_owned();
    for pattern in DROP.iter() {
        text = pattern.replace_all(&text, " ").into_owned();
    }
    text = BREAK.replace_all(&text, "\n").into_owned();
    text = TAG.replace_all(&text, " ").into_owned();
    for (entity, value) in [
        ("&nbsp;", " "),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", "\""),
        ("&#39;", "'"),
        ("&apos;", "'"),
        ("&mdash;", "—"),
        ("&ndash;", "–"),
        ("&hellip;", "…"),
        ("&amp;", "&"),
    ] {
        text = text.replace(entity, value);
    }
    text = NUM
        .replace_all(&text, |c: &Captures| {
            let v = &c[1];
            let n = if let Some(hex) = v.strip_prefix('x') {
                u32::from_str_radix(hex, 16)
            } else {
                v.parse()
            };
            n.ok().and_then(char::from_u32).unwrap_or(' ').to_string()
        })
        .into_owned();
    text = HORIZONTAL.replace_all(&text, " ").into_owned();
    text = LINE.replace_all(&text, "\n").into_owned();
    trim(&text).into()
}

// The limit is checked while streaming, so a valid truncated projection need
// not inflate the rest of a large member merely to discover its full size.
struct Budget<R> {
    inner: R,
    left: usize,
}
impl<R: Read> Read for Budget<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let wanted = out.len().min(self.left.saturating_add(1));
        let n = self.inner.read(&mut out[..wanted])?;
        if n > self.left {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "document content too large",
            ));
        }
        self.left -= n;
        Ok(n)
    }
}
#[derive(Default)]
struct Node {
    tag: String,
    attrs: BTreeMap<String, String>,
    text: String,
    children: Vec<Node>,
    tail: String,
}
impl Node {
    fn attr(&self, key: &str) -> Option<&str> {
        self.attrs.get(key).map(String::as_str)
    }
}
fn attach_text(stack: &mut [Node], text: &str) {
    if let Some(node) = stack.last_mut() {
        if let Some(child) = node.children.last_mut() {
            child.tail.push_str(text)
        } else {
            node.text.push_str(text)
        }
    }
}
fn node(e: &quick_xml::events::BytesStart<'_>) -> Result<Node> {
    let attrs = e
        .attributes()
        .map(|a| {
            let a = a.map_err(|_| Error::Decode("invalid XML attribute"))?;
            let key = a.key.local_name().as_ref().to_owned();
            let v = quick_xml::escape::unescape(&a.value)
                .map_err(|_| Error::Decode("invalid XML entity"))?
                .into_owned();
            Ok((key, v))
        })
        .collect::<Result<_>>()?;
    Ok(Node {
        tag: e.local_name().as_ref().into(),
        attrs,
        ..Default::default()
    })
}
fn xml_blocks(
    reader: impl Read,
    blocks: &[&str],
    seps: &[&str],
    mut emit: impl FnMut(&Node) -> Result<bool>,
) -> Result<()> {
    let mut xml = Reader::from_reader(BufReader::new(Budget {
        inner: reader,
        left: MAX_MEMBER_BYTES,
    }));
    let mut buffer = Vec::new();
    let mut stack: Vec<Node> = Vec::new();
    loop {
        match xml.read_event_into(&mut buffer)? {
            Event::Start(e) => {
                let tag = e.local_name().as_ref().to_owned();
                if !stack.is_empty() || blocks.contains(&tag.as_str()) {
                    if stack.len() >= 1024 {
                        return Err(Error::Decode("XML nesting too deep"));
                    }
                    stack.push(node(&e)?);
                }
            }
            Event::Empty(e) => {
                let n = node(&e)?;
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(n)
                } else if blocks.contains(&n.tag.as_str()) && !emit(&n)? {
                    break;
                }
            }
            Event::End(e) => {
                if let Some(n) = stack.pop() {
                    if let Some(parent) = stack.last_mut() {
                        parent.children.push(n)
                    } else if !emit(&n)? {
                        break;
                    }
                } else if seps.contains(&e.local_name().as_ref()) && !emit(&Node::default())? {
                    break;
                }
            }
            Event::Text(e) => attach_text(&mut stack, &e.xml10_content()),
            Event::CData(e) => attach_text(&mut stack, &e.xml10_content()),
            Event::GeneralRef(e) => {
                let resolved = e
                    .resolve_char_ref()?
                    .or_else(|| match e.as_ref() {
                        "amp" => Some('&'),
                        "lt" => Some('<'),
                        "gt" => Some('>'),
                        "quot" => Some('"'),
                        "apos" => Some('\''),
                        _ => None,
                    })
                    .ok_or(Error::Decode("unknown XML entity"))?;
                attach_text(&mut stack, &resolved.to_string());
            }
            Event::Eof => {
                if !stack.is_empty() {
                    return Err(Error::Decode("incomplete XML"));
                }
                break;
            }
            _ => (),
        }
        buffer.clear();
    }
    Ok(())
}
fn numbered(path: &Path, prefix: &str, suffix: &str) -> Result<Vec<String>> {
    let z = ZipArchive::new(File::open(path)?)?;
    let mut names = Vec::new();
    for name in z.file_names() {
        let name = name.map_err(|_| Error::Decode("invalid zip filename"))?;
        if let Some(n) = name
            .strip_prefix(prefix)
            .and_then(|s| s.strip_suffix(suffix))
        {
            if !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()) {
                if let Ok(number) = n.parse::<u64>() {
                    names.push((number, name.into_owned()));
                }
            }
        }
    }
    names.sort();
    Ok(names.into_iter().map(|(_, n)| n).collect())
}
fn paragraphs(parts: Vec<String>) -> String {
    static NEWLINES: LazyLock<Regex> = LazyLock::new(|| re(r"\n{3,}"));
    let text = parts.join("\n");
    trim(&NEWLINES.replace_all(&text, "\n\n")).into()
}
fn zip_text(
    path: &Path,
    members: Vec<String>,
    blocks: &[&str],
    seps: &[&str],
    format: fn(&Node) -> String,
) -> Result<String> {
    let mut z = ZipArchive::new(File::open(path)?)?;
    let mut parts = Vec::new();
    let mut total = 0;
    for (i, name) in members.iter().enumerate() {
        if i > 0 {
            parts.push(String::new());
        }
        let input = z.by_name(name)?;
        xml_blocks(input, blocks, seps, |n| {
            let text = format(n);
            total += text.chars().count() + 1;
            parts.push(text);
            Ok(total <= MAX_TEXT_CHARS)
        })?;
        if total > MAX_TEXT_CHARS {
            break;
        }
    }
    Ok(paragraphs(parts))
}
fn ooxml(n: &Node) -> String {
    fn walk(n: &Node, out: &mut String) {
        for c in &n.children {
            match c.tag.as_str() {
                "t" => out.push_str(&c.text),
                "tab" => out.push('\t'),
                "br" | "cr" => out.push('\n'),
                "pPr" | "rPr" | "Fallback" | "instrText" | "delText" => (),
                _ => {
                    walk(c, out);
                    if c.tag == "p" {
                        out.push('\n');
                    }
                }
            }
        }
    }
    let mut text = String::new();
    walk(n, &mut text);
    text.trim_end_matches('\n').into()
}
fn odf_inline(n: &Node) -> String {
    let mut out = n.text.clone();
    for c in &n.children {
        match c.tag.as_str() {
            "s" => {
                let count = c
                    .attr("c")
                    .map(|v| v.parse::<i64>().unwrap_or(1).clamp(0, 64) as usize)
                    .unwrap_or(1);
                out.push_str(&" ".repeat(count));
            }
            "tab" => out.push('\t'),
            "line-break" => out.push('\n'),
            "annotation" => (),
            _ => {
                out.push_str(&odf_inline(c));
                if matches!(c.tag.as_str(), "p" | "h") {
                    out.push('\n');
                }
            }
        }
        out.push_str(&c.tail);
    }
    out
}
fn odf_block(n: &Node) -> String {
    if n.tag == "table-row" {
        n.children
            .iter()
            .filter(|c| c.tag == "table-cell")
            .map(|c| trim(&odf_inline(c)).replace('\n', " "))
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\t")
    } else {
        odf_inline(n).trim_end_matches('\n').into()
    }
}
fn si(n: &Node) -> String {
    let mut out = String::new();
    for c in &n.children {
        if c.tag == "t" {
            out.push_str(&c.text)
        } else if c.tag == "r" {
            for t in &c.children {
                if t.tag == "t" {
                    out.push_str(&t.text)
                }
            }
        }
    }
    out
}
fn xlsx(path: &Path) -> Result<String> {
    let mut z = ZipArchive::new(File::open(path)?)?;
    let mut shared = Vec::new();
    if z.file_names()
        .any(|n| n.is_ok_and(|v| v == "xl/sharedStrings.xml"))
    {
        let mut size = 0;
        xml_blocks(z.by_name("xl/sharedStrings.xml")?, &["si"], &[], |n| {
            let text = si(n);
            size += text.chars().count();
            shared.push(text);
            Ok(size <= MAX_TEXT_CHARS)
        })?;
    }
    let mut parts = Vec::new();
    let mut total = 0;
    for (i, name) in numbered(path, "xl/worksheets/sheet", ".xml")?
        .iter()
        .enumerate()
    {
        if i > 0 {
            parts.push(String::new());
        }
        xml_blocks(z.by_name(name)?, &["row"], &[], |row| {
            let mut values = Vec::new();
            let mut size = 0;
            for c in &row.children {
                if size > MAX_TEXT_CHARS {
                    break;
                }
                if c.tag != "c" {
                    continue;
                }
                let kind = c.attr("t");
                let v = if kind == Some("inlineStr") {
                    c.children
                        .iter()
                        .filter(|n| n.tag == "is")
                        .map(si)
                        .collect::<String>()
                } else {
                    let v = c
                        .children
                        .iter()
                        .find(|n| n.tag == "v")
                        .map(|n| n.text.as_str())
                        .unwrap_or("");
                    if kind == Some("s") {
                        v.parse::<usize>()
                            .ok()
                            .and_then(|index| shared.get(index))
                            .map(String::as_str)
                            .unwrap_or(v)
                            .into()
                    } else {
                        v.into()
                    }
                };
                if !trim(&v).is_empty() {
                    let text = trim(&v).replace('\n', " ");
                    size += text.chars().count() + 1;
                    values.push(text);
                }
            }
            let line = values.join("\t");
            if !line.is_empty() {
                total += line.chars().count() + 1;
                parts.push(line);
            }
            Ok(total <= MAX_TEXT_CHARS)
        })?;
        if total > MAX_TEXT_CHARS {
            break;
        }
    }
    Ok(trim(&parts.join("\n")).into())
}

pub fn rtf_text(raw: &str) -> String {
    static TOKEN: LazyLock<Regex> = LazyLock::new(|| {
        re(
            r"\\([a-zA-Z]+)(-?\d+)? ?|\\'([0-9a-fA-F]{2})|\\([^a-zA-Z])|([{}])|[\r\n]+|([^\\{}\r\n]+)",
        )
    });
    const DEST: &[&str] = &[
        "fonttbl",
        "colortbl",
        "stylesheet",
        "info",
        "pict",
        "object",
        "themedata",
        "colorschememapping",
        "latentstyles",
        "datastore",
        "xmlnstbl",
        "listtable",
        "listoverridetable",
        "rsidtbl",
        "generator",
        "filetbl",
        "revtbl",
        "header",
        "headerl",
        "headerr",
        "headerf",
        "footer",
        "footerl",
        "footerr",
        "footerf",
        "fldinst",
    ];
    fn push(out: &mut Vec<u16>, text: &str) {
        out.extend(text.encode_utf16());
    }
    let mut out: Vec<u16> = Vec::new();
    let mut stack = Vec::new();
    let (mut skip, mut uc, mut pending) = (false, 1usize, 0usize);
    let mut encoding = WINDOWS_1252;
    for c in TOKEN.captures_iter(raw) {
        if let Some(brace) = c.get(5) {
            if brace.as_str() == "{" {
                stack.push((skip, uc));
            } else {
                (skip, uc) = stack.pop().unwrap_or((false, 1));
                pending = 0;
            }
        } else if let Some(word) = c.get(1) {
            pending = 0;
            let w = word.as_str();
            let arg = c.get(2).map(|m| m.as_str());
            if DEST.contains(&w) {
                skip = true;
            } else if w == "ansicpg" {
                if let Some(a) = arg {
                    encoding = Encoding::for_label(format!("windows-{a}").as_bytes())
                        .unwrap_or(WINDOWS_1252);
                }
            } else if w == "uc" {
                if let Some(a) = arg {
                    uc = a.parse::<i64>().unwrap_or(1).max(0) as usize;
                }
            } else if w == "u" {
                if let Some(a) = arg {
                    if !skip {
                        let unit = a.parse::<i64>().unwrap_or(0).rem_euclid(65536) as u32;
                        out.push(unit as u16);
                    }
                    pending = uc;
                }
            } else if !skip {
                push(
                    &mut out,
                    match w {
                        "par" | "line" | "sect" | "page" | "row" => "\n",
                        "tab" | "cell" => "\t",
                        "emdash" => "—",
                        "endash" => "–",
                        "bullet" => "•",
                        "lquote" => "‘",
                        "rquote" => "’",
                        "ldblquote" => "“",
                        "rdblquote" => "”",
                        "emspace" | "enspace" | "qmspace" => " ",
                        _ => "",
                    },
                );
            }
        } else if let Some(hex) = c.get(3) {
            if pending > 0 {
                pending -= 1;
            } else if !skip {
                let b = [u8::from_str_radix(hex.as_str(), 16).unwrap()];
                push(&mut out, &encoding.decode(&b).0);
            }
        } else if let Some(symbol) = c.get(4) {
            let s = symbol.as_str();
            if s == "*" {
                skip = true;
            } else if pending > 0 {
                pending -= 1;
            } else if !skip {
                push(
                    &mut out,
                    match s {
                        "\\" => "\\",
                        "{" => "{",
                        "}" => "}",
                        "~" => " ",
                        "_" => "-",
                        "\n" | "\r" => "\n",
                        _ => "",
                    },
                );
            }
        } else if let Some(text) = c.get(6) {
            let t = text.as_str();
            let cut = pending.min(t.chars().count());
            pending -= cut;
            if !skip {
                push(&mut out, &t.chars().skip(cut).collect::<String>());
            }
        }
    }
    static ENDSPACE: LazyLock<Regex> = LazyLock::new(|| re(r"[ \t]+\n"));
    paragraphs(vec![
        ENDSPACE
            .replace_all(&String::from_utf16_lossy(&out), "\n")
            .into_owned(),
    ])
}

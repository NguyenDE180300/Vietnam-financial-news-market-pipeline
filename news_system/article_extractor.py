"""Best-effort full article and image extraction from public news URLs."""
from __future__ import annotations

import html
import gzip
import re
import urllib.request
import zlib
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin

from .cleaner import clean_text


MAX_COMPRESSED_BYTES = 5_000_000
MAX_DECOMPRESSED_BYTES = 20_000_000


def _decode_response_body(raw: bytes, content_encoding: str | None) -> bytes:
    """Decode HTTP content encodings with a post-decompression size limit."""
    encoding = (content_encoding or "").lower().strip()
    if encoding in {"", "identity"}:
        decoded = raw
    elif encoding == "gzip":
        decoded = gzip.decompress(raw)
    elif encoding == "deflate":
        try:
            decoded = zlib.decompress(raw)
        except zlib.error:
            decoded = zlib.decompress(raw, -zlib.MAX_WBITS)
    else:
        raise ValueError(f"Unsupported article content encoding: {encoding}")
    if len(decoded) > MAX_DECOMPRESSED_BYTES:
        raise ValueError("Decompressed article page exceeds 20 MB limit")
    return decoded


@dataclass
class ArticleContent:
    title: str = ""
    content: str = ""
    author: str | None = None
    image_url: str | None = None


class _MetaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.metas: dict[str, str] = {}
        self.title = False
        self.in_title = False
        self.title_text: list[str] = []
        self.in_article = False
        self.depth = 0
        self.article_text: list[str] = []
        self.in_paragraph = False
        self.paragraph_depth = 0
        self.paragraph_text: list[str] = []
        self.paragraphs: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs = dict(attrs)
        if tag == "meta":
            key = attrs.get("property") or attrs.get("name") or attrs.get("itemprop")
            value = attrs.get("content")
            if key and value:
                self.metas[key.lower()] = value.strip()
        if tag == "title":
            self.in_title = True
        if tag == "article" or (tag in {"div", "main", "section"} and
                                 ("article" in (attrs.get("class", "") + " " + attrs.get("id", "")).lower())):
            if not self.in_article:
                self.in_article = True
                self.depth = 1
            elif self.in_article:
                self.depth += 1
        elif self.in_article and tag not in {"img", "br", "hr", "meta", "link", "input"}:
            self.depth += 1
        if self.in_article and tag == "p":
            self.in_paragraph = True
            self.paragraph_depth = self.depth
            self.paragraph_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False
        if self.in_paragraph and tag == "p":
            text = clean_text(" ".join(self.paragraph_text))
            if len(text) >= 35:
                self.paragraphs.append(text)
            self.in_paragraph = False
        if self.in_article and tag not in {"img", "br", "hr", "meta", "link", "input"}:
            self.depth -= 1
            if self.depth <= 0:
                self.in_article = False

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title_text.append(data)
        if self.in_article:
            if self.in_paragraph:
                self.paragraph_text.append(data)
            self.article_text.append(data)


def extract_article(url: str, timeout: int = 20) -> ArticleContent:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; NewsResearchBot/0.1; +https://example.org/bot)"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(MAX_COMPRESSED_BYTES + 1)
        if len(raw) > MAX_COMPRESSED_BYTES:
            raise ValueError("Article page exceeds 5 MB limit")
        charset = response.headers.get_content_charset() or "utf-8"
        content_encoding = response.headers.get("Content-Encoding")
    raw = _decode_response_body(raw, content_encoding)
    document = raw.decode(charset, errors="replace")
    parser = _MetaParser()
    parser.feed(document)
    metas = parser.metas
    content = "\n".join(dict.fromkeys(parser.paragraphs))
    if len(content) < 200:
        # Avoid indexing menus and sidebars if article markup wasn't detected.
        content = "\n".join(dict.fromkeys(
            clean_text(value) for value in re.findall(r"<p\b[^>]*>(.*?)</p>", document, re.I | re.S)
            if len(clean_text(value)) >= 50
        ))
    image = (metas.get("og:image") or metas.get("twitter:image") or
             metas.get("twitter:image:src") or metas.get("image"))
    author = metas.get("author") or metas.get("article:author") or metas.get("byl")
    return ArticleContent(
        title=clean_text(metas.get("og:title") or " ".join(parser.title_text)),
        content=html.unescape(content),
        author=clean_text(author) or None,
        image_url=urljoin(url, image) if image else None,
    )

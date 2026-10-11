"""Lightweight HTML discovery for public HNX and SSC disclosure pages.

This module intentionally stores discovery metadata only. Downloading source
documents to MinIO is a separate pipeline step.
"""

from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = "FinancialDisclosureCollector/0.1 (academic project; contact: project owner)"
REPORT_TERMS = re.compile(
    r"báo cáo tài chính|bctc|financial statements|audited financial", re.IGNORECASE
)
REPORT_TYPE = re.compile(
    r"báo cáo tài chính\s*(?:hợp nhất|riêng|mẹ)?\s*(?:quý\s*\d|bán niên|năm)?|"
    r"bctc\s*(?:quý\s*\d|bán niên|năm)?|financial statements",
    re.IGNORECASE,
)
TICKER = re.compile(r"(?<![A-Z0-9])([A-Z]{3})(?![A-Z0-9])")
DATE_PATTERNS = (
    re.compile(r"\b(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?\b"),
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?\b"),
)
ATTACHMENT_SUFFIXES = (".pdf", ".xls", ".xlsx", ".doc", ".docx", ".zip")
MAX_ATTACHMENT_BYTES = 50 * 1024 * 1024

SOURCE_CONFIG = {
    "hnx": {
        "url": "https://www.hnx.vn/vi-vn/",
        "allowed_hosts": {"hnx.vn", "www.hnx.vn", "owa.hnx.vn"},
    },
    "ssc": {
        "url": "https://congbothongtin.ssc.gov.vn/",
        "allowed_hosts": {"congbothongtin.ssc.gov.vn", "ssc.gov.vn", "www.ssc.gov.vn"},
    },
}


class SourceLayoutError(RuntimeError):
    """Raised when a source page cannot be parsed as a disclosure listing."""


def _allowed_url(url: str, source: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in SOURCE_CONFIG[source]["allowed_hosts"]


def _page_identity(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    path = re.sub(r";jsessionid=[^/]*", "", parsed.path, flags=re.IGNORECASE)
    return (parsed.hostname or "").lower(), path.rstrip("/") or "/"


def _canonical_page_url(url: str) -> str:
    parsed = urlparse(url)
    path = re.sub(r";jsessionid=[^/]*", "", parsed.path, flags=re.IGNORECASE)
    return urlunparse((parsed.scheme, parsed.netloc, path or "/", "", parsed.query, ""))


def _date_from_text(text: str) -> datetime | None:
    for index, pattern in enumerate(DATE_PATTERNS):
        match = pattern.search(text)
        if not match:
            continue
        groups = match.groups()
        if index == 0:
            day, month, year = map(int, groups[:3])
        else:
            year, month, day = map(int, groups[:3])
        hour, minute, second = (int(value or 0) for value in groups[3:6])
        try:
            return datetime(year, month, day, hour, minute, second)
        except ValueError:
            return None
    return None


def _attachment_links(container: Any, page_url: str, source: str) -> list[str]:
    links: set[str] = set()
    for anchor in container.select("a[href]"):
        href = anchor.get("href", "").strip()
        if not href or href.startswith(("#", "javascript:")):
            continue
        candidate = urljoin(page_url, href)
        path = urlparse(candidate).path.lower()
        label = anchor.get_text(" ", strip=True).lower()
        if not _allowed_url(candidate, source):
            continue
        query = urlparse(candidate).query.lower()
        query_looks_like_download = any(
            token in query for token in ("download", "fileid", "file_id", "attachment", "document", "docid")
        )
        same_page = _page_identity(candidate) == _page_identity(page_url) and not query_looks_like_download
        if not same_page and (path.endswith(ATTACHMENT_SUFFIXES) or query_looks_like_download or any(
            token in label for token in ("tải file", "tải về", "download", "đính kèm")
        )):
            links.add(candidate)
    return sorted(links)


def _parse_listing(html: str, page_url: str, source: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    records: dict[str, dict[str, Any]] = {}

    # Table rows are the most reliable unit on the current HNX/SSC listing pages.
    # Fall back to a link's immediate container for future layout variations.
    containers = list(soup.select("table tr"))
    if not containers:
        containers = [a.parent for a in soup.select("a[href]") if a.parent]

    for container in containers:
        row_text = container.get_text(" ", strip=True)
        if not row_text or not REPORT_TERMS.search(row_text):
            continue

        anchors = [a for a in container.select("a[href]") if a.get("href", "").strip()
                   and not a.get("href", "").strip().startswith(("#", "javascript:"))]
        title = next(
            (
                cell.get_text(" ", strip=True)
                for cell in container.select("th, td")
                if REPORT_TERMS.search(cell.get_text(" ", strip=True))
            ),
            row_text[:500],
        )
        report_anchor = next(
            (a for a in anchors if REPORT_TERMS.search(a.get_text(" ", strip=True))),
            None,
        )
        anchor_title = report_anchor.get_text(" ", strip=True) if report_anchor else ""
        if REPORT_TERMS.search(anchor_title):
            title = anchor_title
        detail_url = urljoin(page_url, report_anchor.get("href", "").strip()) if report_anchor else ""
        detail_link_found = bool(detail_url and _allowed_url(detail_url, source)
                                 and _page_identity(detail_url) != _page_identity(page_url))
        source_url = detail_url if detail_link_found else _canonical_page_url(page_url)

        ticker_match = TICKER.search(row_text.upper())
        published_at = _date_from_text(row_text)
        report_type_match = REPORT_TYPE.search(title)
        attachments = _attachment_links(container, page_url, source)
        # A source URL is the most stable identifier when a notice's title/date
        # is corrected by the exchange after its first publication.
        identity = source_url if detail_link_found else (
            f"{_canonical_page_url(page_url)}|{ticker_match.group(1) if ticker_match else ''}|{title}|"
            f"{published_at.isoformat() if published_at else row_text}"
        )
        fingerprint = hashlib.sha256(f"{source}|{identity}".encode("utf-8")).hexdigest()
        records[fingerprint] = {
            "disclosure_id": fingerprint,
            "source": source,
            "source_url": source_url,
            "ticker": ticker_match.group(1) if ticker_match else None,
            "title": title,
            "report_type": report_type_match.group(0).strip() if report_type_match else "Báo cáo tài chính",
            "published_at": published_at,
            "attachment_urls": attachments,
            "raw_metadata": {
                "listing_text": row_text[:2000], "listing_page": _canonical_page_url(page_url),
                "detail_link_found": detail_link_found,
            },
        }

    return list(records.values())


def _next_page(html: str, page_url: str, source: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    candidates = soup.select('a[rel="next"], a[aria-label*="next" i]')
    for anchor in candidates:
        candidate = urljoin(page_url, anchor.get("href", "").strip())
        if candidate != page_url and _allowed_url(candidate, source):
            return candidate
    return None


def _discover_detail_attachments(session: requests.Session, record: dict[str, Any], timeout: int) -> list[str]:
    """Follow a notice page when attachment links are not present in its row."""
    if record["attachment_urls"]:
        return record["attachment_urls"]
    if not record["raw_metadata"].get("detail_link_found"):
        return []
    response = session.get(record["source_url"], timeout=timeout)
    response.raise_for_status()
    if not _allowed_url(response.url, record["source"]):
        return []
    if "text/html" not in response.headers.get("Content-Type", "").lower():
        # Some source pages link directly to a report file.
        return [response.url]
    soup = BeautifulSoup(response.text, "html.parser")
    return _attachment_links(soup, response.url, record["source"])


def _safe_filename(response: requests.Response, attachment_url: str) -> str:
    disposition = response.headers.get("Content-Disposition", "")
    match = re.search(r"filename\*?=(?:UTF-8''|\"?)([^\";]+)", disposition, re.IGNORECASE)
    name = unquote(match.group(1).strip() if match else Path(urlparse(attachment_url).path).name)
    name = name.replace("\\", "_").replace("/", "_")
    name = re.sub(r"[^\w.() -]+", "_", name, flags=re.UNICODE).strip(" .")
    return name[:180] or "attachment.bin"


def _download_attachment(
    session: requests.Session, source: str, record: dict[str, Any], attachment_url: str,
    output_dir: Path, timeout: int,
) -> dict[str, Any]:
    if not _allowed_url(attachment_url, source):
        raise ValueError(f"Attachment host is not allowed: {urlparse(attachment_url).hostname}")
    response = session.get(attachment_url, timeout=timeout, stream=True)
    response.raise_for_status()
    if not _allowed_url(response.url, source):
        response.close()
        raise ValueError(f"Attachment redirected to an unapproved host: {urlparse(response.url).hostname}")
    if "text/html" in response.headers.get("Content-Type", "").lower():
        response.close()
        raise ValueError("Attachment endpoint returned HTML instead of a document")

    content_length = response.headers.get("Content-Length")
    if content_length and int(content_length) > MAX_ATTACHMENT_BYTES:
        response.close()
        raise ValueError(f"Attachment exceeds {MAX_ATTACHMENT_BYTES} bytes")

    ticker = record["ticker"] or "unknown"
    target_dir = output_dir / source / ticker
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = _safe_filename(response, attachment_url)
    temp_path = target_dir / f".{record['disclosure_id'][:12]}-{filename}.part"
    digest = hashlib.sha256()
    size = 0
    try:
        with temp_path.open("wb") as destination:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                size += len(chunk)
                if size > MAX_ATTACHMENT_BYTES:
                    raise ValueError(f"Attachment exceeds {MAX_ATTACHMENT_BYTES} bytes")
                digest.update(chunk)
                destination.write(chunk)
        if size == 0:
            raise ValueError("Downloaded attachment is empty")
        # Content-addressed filename avoids overwriting a changed report.
        final_path = target_dir / f"{record['disclosure_id'][:12]}-{digest.hexdigest()[:12]}-{filename}"
        if final_path.exists():
            temp_path.unlink()
        else:
            os.replace(temp_path, final_path)
        return {
            "url": attachment_url,
            "path": str(final_path),
            "sha256": digest.hexdigest(),
            "size_bytes": size,
            "content_type": response.headers.get("Content-Type"),
        }
    finally:
        response.close()
        if temp_path.exists():
            temp_path.unlink()


def _download_record_attachments(
    session: requests.Session, record: dict[str, Any], output_dir: Path, timeout: int,
) -> list[dict[str, Any]]:
    results = []
    attachment_urls = _discover_detail_attachments(session, record, timeout)
    record["attachment_urls"] = attachment_urls
    for attachment_url in attachment_urls[:10]:
        try:
            results.append(
                _download_attachment(session, record["source"], record, attachment_url, output_dir, timeout)
            )
        except Exception as exc:  # Keep one failed document from discarding other notices.
            results.append({"url": attachment_url, "error": str(exc)[:1000]})
    return results


def _collect_ssc_with_browser(
    lookback_days: int, download_dir: str, max_downloads: int | None = None,
) -> list[dict[str, Any]]:
    """Read SSC's Oracle ADF table and click each row's PDF download action.

    SSC returns the document as a binary response to an ADF POST. There is no
    stable per-file GET URL to extract from the listing HTML, so use a browser
    session to preserve the server-side view state and download response.
    """
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright

    source_url = "https://congbothongtin.ssc.gov.vn/"
    timeout_ms = int(os.getenv("COLLECTOR_HTTP_TIMEOUT_SECONDS", "30")) * 1000
    output_dir = Path(download_dir).expanduser().resolve()
    cutoff = datetime.now() - timedelta(days=lookback_days)
    records_by_id: dict[str, dict[str, Any]] = {}
    attempted = 0

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)
        try:
            page.goto(source_url, wait_until="networkidle", timeout=timeout_ms)
            page.locator('a[id^="pt9:t1:"][id$="cil4z"]').first.wait_for(timeout=timeout_ms)

            for _page_number in range(1137):
                download_buttons = page.locator('a[id^="pt9:t1:"][id$="cil4z"]')
                row_count = download_buttons.count()
                page_records: list[dict[str, Any]] = []

                for row_index in range(row_count):
                    button = download_buttons.nth(row_index)
                    row = button.locator("xpath=ancestor::tr[1]")
                    cells = [cell.strip() for cell in row.locator("td").all_inner_texts()]
                    if len(cells) < 7:
                        continue
                    row_text = " ".join(cells)
                    if not REPORT_TERMS.search(row_text):
                        continue

                    published_at = _date_from_text(cells[6])
                    ticker_value = cells[2].strip().upper()
                    ticker_match = TICKER.search(ticker_value)
                    ticker = ticker_match.group(1) if ticker_match else None
                    title = cells[3].strip() or row_text[:500]
                    listing_text = row_text[:2000]
                    identity = (
                        f"{source_url}|{ticker or ''}|{title}|"
                        f"{published_at.isoformat() if published_at else listing_text}"
                    )
                    fingerprint = hashlib.sha256(f"ssc|{identity}".encode("utf-8")).hexdigest()
                    record = {
                        "disclosure_id": fingerprint,
                        "source": "ssc",
                        "source_url": source_url,
                        "ticker": ticker,
                        "title": title,
                        "report_type": (REPORT_TYPE.search(title).group(0).strip()
                                        if REPORT_TYPE.search(title) else "Báo cáo tài chính"),
                        "published_at": published_at,
                        "attachment_urls": [],
                        "local_files": [],
                        "raw_metadata": {
                            "listing_page": source_url,
                            "listing_text": listing_text,
                            "exchange": cells[1].strip(),
                            "company_name": cells[4].strip(),
                            "abstract": cells[5].strip(),
                            "detail_link_found": False,
                            "download_method": "Oracle ADF row action (POST)",
                        },
                    }
                    records_by_id[fingerprint] = record
                    page_records.append(record)

                dated = [r["published_at"] for r in page_records if r["published_at"]]
                if dated and max(dated) < cutoff:
                    break

                for row_index, record in enumerate(page_records):
                    if record["published_at"] and record["published_at"] < cutoff:
                        continue
                    if max_downloads is not None and attempted >= max_downloads:
                        break
                    attempted += 1
                    target_dir = output_dir / "ssc" / (record["ticker"] or "unknown")
                    target_dir.mkdir(parents=True, exist_ok=True)
                    existing = next(target_dir.glob(f"{record['disclosure_id'][:12]}-*"), None)
                    if existing and existing.is_file():
                        digest = hashlib.sha256(existing.read_bytes()).hexdigest()
                        record["local_files"].append({
                            "url": page.url, "path": str(existing), "filename": existing.name,
                            "sha256": digest, "size_bytes": existing.stat().st_size,
                            "content_type": "application/pdf", "download_method": "Oracle ADF POST",
                        })
                        continue

                    button = download_buttons.nth(row_index)
                    temp_path: Path | None = None
                    try:
                        with page.expect_download(timeout=timeout_ms) as pending:
                            button.click(timeout=timeout_ms)
                        download = pending.value
                        filename = _safe_download_name(download.suggested_filename)
                        temp_path = target_dir / f".{record['disclosure_id'][:12]}-{filename}.part"
                        download.save_as(str(temp_path))
                        if temp_path.stat().st_size > MAX_ATTACHMENT_BYTES:
                            raise ValueError(f"Attachment exceeds {MAX_ATTACHMENT_BYTES} bytes")
                        digest = hashlib.sha256()
                        size = 0
                        with temp_path.open("rb") as source_file:
                            signature = source_file.read(5)
                            if signature != b"%PDF-":
                                raise ValueError("SSC download response is not a PDF")
                            digest.update(signature)
                            size = len(signature)
                            while chunk := source_file.read(1024 * 1024):
                                size += len(chunk)
                                if size > MAX_ATTACHMENT_BYTES:
                                    raise ValueError(f"Attachment exceeds {MAX_ATTACHMENT_BYTES} bytes")
                                digest.update(chunk)
                        if size <= 5:
                            raise ValueError("Downloaded PDF is empty")
                        final_path = target_dir / (
                            f"{record['disclosure_id'][:12]}-{digest.hexdigest()[:12]}-{filename}"
                        )
                        os.replace(temp_path, final_path)
                        record["local_files"].append({
                            "url": download.url, "path": str(final_path), "filename": filename,
                            "sha256": digest.hexdigest(), "size_bytes": size,
                            "content_type": "application/pdf", "download_method": "Oracle ADF POST",
                        })
                    except Exception as exc:
                        if temp_path and Path(temp_path).exists():
                            Path(temp_path).unlink()
                        record["local_files"].append({"url": page.url, "error": str(exc)[:1000]})
                    page.wait_for_timeout(500)

                if max_downloads is not None and attempted >= max_downloads:
                    break

                next_page = page.locator('a[id="pt9:t1::nb_nx"]')
                if not next_page.count() or not next_page.is_visible():
                    break
                first_row = page.locator('a[id^="pt9:t1:"][id$="cil4z"]').first
                previous_text = first_row.locator("xpath=ancestor::tr[1]").inner_text()
                try:
                    next_page.click(timeout=timeout_ms)
                    page.wait_for_function(
                        "old => { const a=document.querySelector('a[id^=\\\"pt9:t1:\\\"][id$=\\\"cil4z\\\"]'); "
                        "return a && a.closest('tr').innerText !== old; }",
                        arg=previous_text,
                        timeout=timeout_ms,
                    )
                except PlaywrightTimeoutError:
                    break
        finally:
            browser.close()

    records = list(records_by_id.values())
    if not records:
        raise SourceLayoutError("ssc: no financial-report rows found in the rendered disclosure table")
    return [r for r in records if r["published_at"] is None or r["published_at"] >= cutoff]


def _safe_download_name(name: str) -> str:
    filename = Path(name.replace("\\", "/")).name
    filename = re.sub(r"[^\w.() -]+", "_", filename, flags=re.UNICODE).strip(" .")[:180]
    if not filename:
        return "report.pdf"
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    return filename


def collect_disclosures(
    source: str, lookback_days: int = 7, download_dir: str | None = None,
    max_downloads: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch listing pages and optionally download report attachments locally."""
    if source not in SOURCE_CONFIG:
        raise ValueError(f"Unsupported source: {source}")

    if source == "ssc" and download_dir:
        return _collect_ssc_with_browser(lookback_days, download_dir, max_downloads)

    import os

    config = SOURCE_CONFIG[source]
    url = os.getenv(f"{source.upper()}_DISCLOSURE_URL", config["url"])
    timeout = int(os.getenv("COLLECTOR_HTTP_TIMEOUT_SECONDS", "30"))
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "vi,en;q=0.8"})
    records_by_id: dict[str, dict[str, Any]] = {}
    next_url: str | None = url
    seen_pages: set[str] = set()
    # Follow standard next-page links when the source exposes them. Some pages
    # use JavaScript form posts; those need a source-specific adapter.
    for _ in range(10):
        if not next_url or next_url in seen_pages:
            break
        seen_pages.add(next_url)
        response = session.get(next_url, timeout=timeout)
        response.raise_for_status()
        if "text/html" not in response.headers.get("Content-Type", "").lower():
            raise SourceLayoutError(f"{source}: expected HTML at {next_url}")
        parsed = _parse_listing(response.text, response.url, source)
        records_by_id.update({record["disclosure_id"]: record for record in parsed})
        next_url = _next_page(response.text, response.url, source)

    records = list(records_by_id.values())
    if not records:
        raise SourceLayoutError(
            f"{source}: no financial-report rows were found at {response.url}. "
            "The site may require JavaScript or its page layout may have changed."
        )

    # Preserve undated rows because public pages sometimes omit the publish date;
    # filter only when a date is actually available.
    from datetime import timedelta

    cutoff = datetime.now() - timedelta(days=lookback_days)
    recent_records = [r for r in records if r["published_at"] is None or r["published_at"] >= cutoff]
    if download_dir:
        output_dir = Path(download_dir).expanduser().resolve()
        for record in recent_records:
            record["local_files"] = _download_record_attachments(session, record, output_dir, timeout)
    else:
        for record in recent_records:
            record["local_files"] = []
    return recent_records

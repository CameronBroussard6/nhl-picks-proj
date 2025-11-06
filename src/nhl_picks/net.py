from __future__ import annotations
import io
from typing import Optional, Dict, Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

UA = "nhl-picks/1.0 (+https://github.com)"

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "application/json, text/csv;q=0.9, */*;q=0.1",
    })
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def _proxy_url(url: str) -> str:
    # r.jina.ai expects the original scheme after the slash.
    if url.startswith("https://"):
        return "https://r.jina.ai/https://" + url[len("https://"):]
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://"):]
    return "https://r.jina.ai/https://" + url

def get_json(url: str, *, params: Optional[Dict[str, Any]] = None, timeout: int = 25, allow_proxy: bool = True) -> Any:
    s = _session()
    # 1) direct
    r = s.get(url, params=params, timeout=timeout)
    if r.ok:
        return r.json()
    # 2) optional proxy fallback
    if allow_proxy:
        rp = s.get(_proxy_url(url), params=params, timeout=timeout)
        rp.raise_for_status()
        return rp.json()
    r.raise_for_status()

def get_bytes(url: str, *, params: Optional[Dict[str, Any]] = None, timeout: int = 30, allow_proxy: bool = True) -> bytes:
    s = _session()
    r = s.get(url, params=params, timeout=timeout)
    if r.ok:
        return r.content
    if allow_proxy:
        rp = s.get(_proxy_url(url), params=params, timeout=timeout)
        rp.raise_for_status()
        return rp.content
    r.raise_for_status()

def read_csv_safely(url: str, *, params: Optional[Dict[str, Any]] = None, allow_proxy: bool = True) -> pd.DataFrame:
    data = get_bytes(url, params=params, allow_proxy=allow_proxy)
    return pd.read_csv(io.BytesIO(data))

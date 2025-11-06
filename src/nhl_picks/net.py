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
    s.headers.update({"User-Agent": UA})
    retry = Retry(
        total=5,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def _proxy_url(url: str) -> str:
    # r.jina.ai expects the ORIGINAL scheme after the slash.
    # e.g., https://r.jina.ai/https://example.com/path?x=1
    if url.startswith("https://"):
        return "https://r.jina.ai/https://" + url[len("https://"):]
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://"):]
    # default to https scheme if missing
    return "https://r.jina.ai/https://" + url


def get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 25) -> Any:
    s = _session()
    # 1) try direct
    try:
        r = s.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        # 2) try proxy
        r = s.get(_proxy_url(url), params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()

def get_bytes(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> bytes:
    s = _session()
    try:
        r = s.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.content
    except Exception:
        r = s.get(_proxy_url(url), params=params, timeout=timeout)
        r.raise_for_status()
        return r.content

def read_csv_safely(url: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    data = get_bytes(url, params=params)
    return pd.read_csv(io.BytesIO(data))

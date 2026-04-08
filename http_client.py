import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config_loader import load_config


def _build_retry(cfg):
    http_cfg = cfg["http"]
    return Retry(
        total=http_cfg["max_retries"],
        backoff_factor=http_cfg["backoff_factor"],
        status_forcelist=http_cfg["retry_statuses"],
        allowed_methods=http_cfg["retry_methods"],
        raise_on_status=False,
    )


def get_http_session():
    cfg = load_config()
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=_build_retry(cfg))
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": cfg["http"]["user_agent"]})
    return session


def _raise_for_bad_response(resp, url):
    if resp.ok:
        return
    body = (resp.text or "").replace("\n", " ")[:500]
    raise RuntimeError(f"Erreur HTTP pour {url} | statut={resp.status_code} | corps={body}")


def http_get_json(url, session=None):
    cfg = load_config()
    sess = session or get_http_session()
    resp = sess.get(url, timeout=cfg["http"]["timeout_seconds"])
    _raise_for_bad_response(resp, url)
    try:
        return resp.json()
    except ValueError as exc:
        body = (resp.text or "").replace("\n", " ")[:500]
        raise RuntimeError(f"Reponse JSON invalide pour {url} | corps={body}") from exc


def http_get_bytes(url, session=None):
    cfg = load_config()
    sess = session or get_http_session()
    resp = sess.get(url, timeout=cfg["http"]["timeout_seconds"])
    _raise_for_bad_response(resp, url)
    return resp.content


def http_get_text(url, session=None):
    cfg = load_config()
    sess = session or get_http_session()
    resp = sess.get(url, timeout=cfg["http"]["timeout_seconds"])
    _raise_for_bad_response(resp, url)
    return resp.text

from pathlib import Path
import yaml


def _get_key(cfg, key_path):
    cur = cfg
    for part in key_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(key_path)
        cur = cur[part]
    return cur


def _validate_config(cfg):
    if not isinstance(cfg, dict):
        raise ValueError("Le contenu YAML doit etre un dictionnaire de configuration.")

    required = {
        "paths.download_root": str,
        "paths.download_files_subdir": str,
        "paths.load_log_file": str,
        "paths.snapshot_source_dir": str,
        "paths.run_log_dir": str,
        "paths.checkpoint_file": str,
        "openalex.api_base_url": str,
        "openalex.s3_base_url": str,
        "openalex.download.run_mode": str,
        "openalex.download.manifest_name": str,
        "openalex.download.max_files_per_dataset": (int, type(None)),
        "http.timeout_seconds": int,
        "http.max_retries": int,
        "http.backoff_factor": (int, float),
        "http.retry_statuses": list,
        "http.retry_methods": list,
        "http.user_agent": str,
        "api_extract.typeres": str,
        "api_extract.base": str,
        "api_extract.params": dict,
        "api_extract.params.per_page": str,
        "snapshot_ingest.file_prefix": str,
        "snapshot_ingest.npool": int,
        "snapshot_ingest.tables.data_table": str,
        "snapshot_ingest.tables.ref_table": str,
        "snapshot_ingest.value_prefixes": dict,
        "snapshot_ingest.value_limits": dict,
        "oracle.connection_api": str,
        "oracle.connection_snapshot": str,
        "oracle.embedded_sql_src_path": str,
    }

    for key_path, expected_type in required.items():
        try:
            value = _get_key(cfg, key_path)
        except KeyError:
            raise ValueError(f"Cle de configuration manquante : {key_path}") from None

        if not isinstance(value, expected_type):
            if isinstance(expected_type, tuple):
                type_name = " ou ".join(t.__name__ for t in expected_type)
            else:
                type_name = expected_type.__name__
            raise ValueError(
                f"Type invalide pour '{key_path}' : attendu {type_name}, recu {type(value).__name__}"
            )

    run_mode = _get_key(cfg, "openalex.download.run_mode")
    if run_mode not in ("fresh", "resume"):
        raise ValueError("Valeur invalide pour 'openalex.download.run_mode' : attendu 'fresh' ou 'resume'")


def load_config(config_path="config/default.yaml"):
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    _validate_config(cfg)
    return cfg

# Recuperation du fichier contenant la liste des fichiers du SNAPSHOT
import os
import time as ti
from config_loader import load_config
from http_client import get_http_session, http_get_bytes, http_get_json, http_get_text
from run_logger import RunLogger
from checkpoint import load_checkpoint, save_checkpoint


def cp_url(url, file="", mode="w", copy=False, ret=False, prt=False):
    """
    Fonction de copie d'un fichier du SNAPSHOT dans un dossier local
    """
    t0 = ti.time()
    if ret is True:
        fout = http_get_json(url, session=HTTP_SESSION)
    else:
        fout = http_get_bytes(url, session=HTTP_SESSION) if mode == "wb" else http_get_text(url, session=HTTP_SESSION) if mode == "w" else None
    if copy is True:
        with open(file, mode=mode) as f:
            f.write(fout)
    if ret is True:
        return fout

    t1 = ti.time()
    if prt is True:
        print("Copie locale du fichier " + file + " : Duree = %f\n" % (t1 - t0))


cfg = load_config()
HTTP_SESSION = get_http_session()
RUN_LOGGER = RunLogger("OpenAlex_GetSnap_20241231")

url = cfg["openalex"]["s3_base_url"]
path = cfg["paths"]["download_root"]
files_subdir = cfg["paths"]["download_files_subdir"]
load_log_file = cfg["paths"]["load_log_file"]
lst_data = cfg["openalex"]["snapshot_datasets"]
run_mode = cfg["openalex"]["download"]["run_mode"]
manifest_name = cfg["openalex"]["download"]["manifest_name"]
max_files_per_dataset = cfg["openalex"]["download"]["max_files_per_dataset"]

os.makedirs(path, exist_ok=True)
os.makedirs(os.path.join(path, files_subdir), exist_ok=True)

t0 = ti.strftime("%H:%M:%S", ti.gmtime())
with open(load_log_file, "w") as f:
    f.write(f"Heure de lancement : {t0}\n")

if run_mode == "fresh":
    CKPT = {}
    save_checkpoint(CKPT)
elif run_mode == "resume":
    CKPT = load_checkpoint()
else:
    raise ValueError("run_mode invalide: utiliser 'fresh' ou 'resume'")

CKPT.setdefault("snapshot_download", {})
CKPT["snapshot_download"].setdefault("datasets", {})

RUN_LOGGER.info("run_start", {"datasets": lst_data, "output_root": path, "run_mode": run_mode})

try:
    for l in lst_data:
        dataset_state = CKPT["snapshot_download"]["datasets"].setdefault(l, {"downloaded_urls": []})
        downloaded_urls = set(dataset_state.get("downloaded_urls", []))
        RUN_LOGGER.info("dataset_start", {"dataset": l, "already_downloaded": len(downloaded_urls)})

        manifest_file = os.path.join(path, f"{l}_{manifest_name}")
        lst_load = cp_url(url + "data/" + l + "/" + manifest_name, manifest_file, ret=True)["entries"]

        for i, load in enumerate(lst_load):
            if max_files_per_dataset is not None and i >= max_files_per_dataset:
                RUN_LOGGER.info("dataset_limit_reached", {"dataset": l, "max_files": max_files_per_dataset})
                break

            source_url = load["url"]
            if source_url in downloaded_urls:
                RUN_LOGGER.info("file_skip_checkpoint", {"dataset": l, "source_url": source_url})
                continue

            url_file = source_url.replace("s3://openalex/", url)
            lf = source_url.split("/")
            ln = lf[4] + "_" + lf[5][13:].replace("-", "") + "_" + lf[6]
            local_file = os.path.join(path, files_subdir, ln)
            t0 = ti.time()

            try:
                cp_url(url_file, local_file, mode="wb", copy=True)
            except Exception as exc:
                RUN_LOGGER.error("file_download_failed", {"dataset": l, "source_url": source_url, "error": str(exc)})
                save_checkpoint(CKPT)
                raise

            t1 = ti.time()
            with open(load_log_file, "a") as f:
                f.write(str({**{"file": ln[:-3]}, **load["meta"], **{"duree": round(t1 - t0, 3)}}) + "\n")

            downloaded_urls.add(source_url)
            dataset_state["downloaded_urls"] = list(downloaded_urls)
            save_checkpoint(CKPT)

            RUN_LOGGER.info(
                "file_downloaded",
                {
                    "dataset": l,
                    "source_url": source_url,
                    "local_file": local_file,
                    "duration_seconds": round(t1 - t0, 3),
                },
            )
            print(url_file + " => " + local_file)

        RUN_LOGGER.info("dataset_end", {"dataset": l, "downloaded_total": len(downloaded_urls)})

except Exception as exc:
    RUN_LOGGER.error("run_failed", {"error": str(exc)})
    raise
else:
    RUN_LOGGER.info("run_end", {"status": "success"})

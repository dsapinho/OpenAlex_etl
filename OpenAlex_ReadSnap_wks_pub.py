import os
import multiprocessing as mp
import time as ti

import Utils_wks_pub as lo
from config_loader import load_config

cfg = load_config()

lst_paths = {
    "id": {"path": "id", "form": "str_50"},
    "doi": {"path": "doi", "form": "str_500"},
    "pmid": {"path": ["ids", "pmid"], "form": "str_50"},
    "title": {"path": "title", "form": "str_4000"},
    "publication_date": {"path": "publication_date", "form": "str_20"},
    "publication_year": {"path": "publication_year", "form": "num"},
    "typedoc": {"path": "type", "form": "str_50"},
    "type_crossref": {"path": "type_crossref", "form": "str_50"},
    "language": {"path": "language", "form": "str_10"},
    "is_oa": {"path": ["open_access", "is_oa"], "form": "bool"},
    "oa_status": {"path": ["open_access", "oa_status"], "form": "str_50"},
    "support_nb": {"path": "locations_count", "form": "num"},
    "support_id": {"path": ["primary_location", "source", "id"], "form": "str_50"},
    "support_title": {"path": ["primary_location", "source", "display_name"], "form": "str_4000"},
    "support_url": {"path": ["primary_location", "landing_page_url"], "form": "str_4000"},
    "support_url_pdf": {"path": ["primary_location", "pdf_url"], "form": "str_4000"},
    "issn_l": {"path": ["primary_location", "source", "issn_l"], "form": "str_50"},
    "issn": {"path": ["primary_location", "source", "issn"], "form": "str_500"},
    "support_is_oa": {"path": ["primary_location", "is_oa"], "form": "bool"},
    "support_is_in_doaj": {"path": ["primary_location", "source", "is_in_doaj"], "form": "bool"},
    "support_type": {"path": ["primary_location", "source", "type"], "form": "str_50"},
    "support_version": {"path": ["primary_location", "version"], "form": "str_100"},
    "support_is_accepted": {"path": ["primary_location", "is_accepted"], "form": "bool"},
    "support_is_published": {"path": ["primary_location", "is_published"], "form": "bool"},
    "cit_nb": {"path": "cited_by_count", "form": "num"},
    "cit_prct_year_min": {"path": ["cited_by_percentile_year", "min"], "form": "float"},
    "cit_prct_year_max": {"path": ["cited_by_percentile_year", "max"], "form": "float"},
    "is_retracted": {"path": "is_retracted", "form": "bool"},
    "is_paratext": {"path": "is_paratext", "form": "bool"},
    "has_fulltext": {"path": "has_fulltext", "form": "bool"},
}

pref = cfg["snapshot_ingest"]["value_prefixes"]
lims = cfg["snapshot_ingest"]["value_limits"]

# Connecteur Oracle
conopa = cfg["oracle"]["connection_snapshot"]

dir = cfg["paths"]["snapshot_source_dir"]
list_files = os.listdir(dir)
list_files = [{"nfic": i, "nomfic": t.split(".")[0]} for i, t in enumerate(list_files) if t.split("_")[0] == cfg["snapshot_ingest"]["file_prefix"]]

npool = cfg["snapshot_ingest"]["npool"]
llstf = lo.sub_list(list_files, nelts=npool)

tab_params = {"pref": pref, "lims": lims}
tabs = {
    "table": cfg["snapshot_ingest"]["tables"]["data_table"],
    "tabref": cfg["snapshot_ingest"]["tables"]["ref_table"],
    "lst_paths": lst_paths,
    "connex": conopa,
}


def trt_pool(lf):
    print("Fichier : " + str(lf))
    io_gz = lo.read_gz(dir, lf)
    lo.trt_fic(lf, io_gz, **tabs, **tab_params)


if __name__ == "__main__":
    print("AVEC multiprocessing process")
    t0 = ti.time()
    lo.create_tables(**tabs)
    for lstf in llstf:
        print("Liste de fichiers : " + str(lstf))
        with mp.Pool(len(lstf)) as p:
            p.map(trt_pool, lstf)
    lo.create_index(tabs["table"], connex=conopa)
    t1 = ti.time()
    print("Traitement global avec multiprocessing process : Duree = %f\n" % (t1 - t0))

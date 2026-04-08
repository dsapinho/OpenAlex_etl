# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 18:22:32 2025

@author: davsa
"""

import Extract_API as opa
import sql_admin as sa
from config_loader import load_config

cfg = load_config()

typeres = cfg["api_extract"]["typeres"]
base = cfg["api_extract"]["base"]
params = cfg["api_extract"]["params"]
opa_erc = opa.extract_OpenAlex(base, params, typeres=typeres)

# Connecteur Oracle
conopa = cfg["oracle"]["connection_api"]

table_cfg = cfg["oracle"]["api_table"]
table_name = table_cfg["name"]
lstvars = table_cfg["create_columns"]
insert_sql = table_cfg["insert_sql"]

sa.sql_create(table_name, "l", lstvars, connex=conopa, run=True)

opa_input = [
    {
        "id": o["id"][21:],
        "funder_id": og["funder"][21:],
        "award_id": og["award_id"],
        "updated_date": o["updated_date"][:10],
    }
    for o in opa_erc["results"]
    for og in o["grants"]
]

c = conopa.cursor()

for o in opa_input:
    c.execute(insert_sql, o)
conopa.commit()
c.close()

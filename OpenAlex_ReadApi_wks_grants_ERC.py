# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 18:22:32 2025

@author: davsa
"""

import Extract_API as opa
import sql_admin as sa

#FUNDER_ID Horizon EUrope / H2020 /FP7 in ('F4320338453','F4320338335','F4320338352')

typeres="res"
base = "works"
params = {"filter" : {"grants.funder":"F4320338453|F4320338335|F4320338352"},
          "select" : "id,grants,updated_date",
          "per_page" : "200",
          "cursor" : "*"}
opa_erc=opa.extract_OpenAlex(base, params, typeres=typeres)

import cx_Oracle as cx
# Connecteur Oracle
conopa="XXX"

lstvars=["ID VARCHAR2(50 CHAR)","FUNDER_ID VARCHAR2(50 CHAR)","AWARD_ID VARCHAR2(500 CHAR)","UPDATED_DATE VARCHAR2(50 CHAR)"]

sa.sql_create("WORKS_GRANTS_ERC_API_20250123","l",lstvars,connex=conopa,run=True)

opa_input=[{"id":o["id"][21:],"funder_id":og["funder"][21:],"award_id":og["award_id"],"updated_date":o["updated_date"][:10]} for o in opa_erc["results"] for og in o["grants"]]

req="INSERT INTO WORKS_GRANTS_ERC_API_20250123 VALUES (:id,:funder_id,:award_id,:updated_date)"

c=conopa.cursor()

for o in opa_input:
    c.execute(req,o)
conopa.commit()
c.close()
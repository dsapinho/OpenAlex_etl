import Utils_wks_pub as lo
import multiprocessing as mp
import time as ti

lst_paths={"id":{"path":"id","form":"str_50"},"doi":{"path":"doi","form":"str_500"},"pmid":{"path":["ids","pmid"],"form":"str_50"}
           ,"title":{"path":"title","form":"str_4000"}
           #,"volume":{"path":["biblio","volume"],"form":"str_50"},"issue":{"path":["biblio","issue"],"form":"str_50"}
           #,"first_page":{"path":["biblio","first_page"],"form":"str_50"},"last_page":{"path":["biblio","last_page"],"form":"str_30"}
           ,"publication_date":{"path":"publication_date","form":"str_20"},"publication_year":{"path":"publication_year","form":"num"}
           ,"typedoc":{"path":"type","form":"str_50"},"type_crossref":{"path":"type_crossref","form":"str_50"},"language":{"path":"language","form":"str_10"}
           ,"is_oa":{"path":["open_access","is_oa"],"form":"bool"},"oa_status":{"path":["open_access","oa_status"],"form":"str_50"}
           ,"support_nb":{"path":"locations_count","form":"num"},"support_id":{"path":["primary_location","source","id"],"form":"str_50"}
           ,"support_title":{"path":["primary_location","source","display_name"],"form":"str_4000"}
           ,"support_url":{"path":["primary_location","landing_page_url"],"form":"str_4000"}
           ,"support_url_pdf":{"path":["primary_location","pdf_url"],"form":"str_4000"}
           ,"issn_l":{"path":["primary_location","source","issn_l"],"form":"str_50"}
           ,"issn":{"path":["primary_location","source","issn"],"form":"str_500"}
           ,"support_is_oa":{"path":["primary_location","is_oa"],"form":"bool"}
           ,"support_is_in_doaj":{"path":["primary_location","source","is_in_doaj"],"form":"bool"}
           ,"support_type":{"path":["primary_location","source","type"],"form":"str_50"}
           ,"support_version":{"path":["primary_location","version"],"form":"str_100"}
           ,"support_is_accepted":{"path":["primary_location","is_accepted"],"form":"bool"}
           ,"support_is_published":{"path":["primary_location","is_published"],"form":"bool"}
           ,"cit_nb":{"path":"cited_by_count","form":"num"}
           ,"cit_prct_year_min":{"path":["cited_by_percentile_year","min"],"form":"float"}
           ,"cit_prct_year_max":{"path":["cited_by_percentile_year","max"],"form":"float"}
           ,"is_retracted":{"path":"is_retracted","form":"bool"},"is_paratext":{"path":"is_paratext","form":"bool"}
           ,"has_fulltext":{"path":"has_fulltext","form":"bool"}}

pref={"id":"https://openalex.org/","support_id":"https://openalex.org/","doi":"https://doi.org/","orcid":"https://orcid.org/"
      ,"ror":"https://ror.org/","wikidata":"https://www.wikidata.org/wiki/","pmid":"https://pubmed.ncbi.nlm.nih.gov/"}
lims={"title":4000,"support_title":4000}

# Connecteur Oracle
conopa="XXXX"

import os
#dir=r"P:\Espace Commun\OpenAlex\snapshot\20240125\files"
#dir=r"P:\Espace Commun\OpenAlex\snapshot\20240327\files"
dir=r"P:\Espace Commun\OpenAlex_snap\snapshot\20241231\files"
# Fichiers petite taille (5Ko)
#list_files=[{"nfic":r,"nomfic":"works_202305"+ str(r) +"_part_000"} for r in range(21)[14:]]

#list_files=[{"nfic":0,"nomfic":"works_20231117_part_000"}]

# Fichiers taille standate (300 à 800Mo)
#list_files=[{"nfic":r,"nomfic":"works_20231120_part_01"+str(r)} for r in range(10)]
# Liste complète des fichiers à traiter
list_files=os.listdir(dir)
#list_files=[{"nfic":i,"nomfic":t.split(".")[0]} for i,t in enumerate(lstf) if t.split("_")[0]=="works"]# and not t.split("_")[1]=="20231120"]
list_files=[{"nfic":i,"nomfic":t.split(".")[0]} for i,t in enumerate(list_files) if t.split("_")[0]=="works"]

npool=60
llstf=lo.sub_list(list_files,nelts=npool)
#print(llstf)

tab_params={"pref":pref,"lims":lims}

tabs={"table":"WORKS_SNAP_20241231","tabref":"WORKS_REF_NOMFIC_20241231","lst_paths":lst_paths,"connex":conopa}

def trt_pool(lf):
    print("Fichier : " + str(lf))
    io_gz=lo.read_gz(dir,lf)
    lo.trt_fic(lf,io_gz,**tabs,**tab_params)  

if __name__=='__main__':
    print("AVEC multiprocessing process")
    t0=ti.time()
    lo.create_tables(**tabs)
    for lstf in llstf:
      print("Liste de fichiers : " + str(lstf))
      with mp.Pool(len(lstf)) as p:
            p.map(trt_pool,lstf)
    lo.create_index(tabs["table"],connex=conopa)
    t1=ti.time()
    print("Traitement global avec multiprocessing process : Durée = %f\n" %(t1-t0))

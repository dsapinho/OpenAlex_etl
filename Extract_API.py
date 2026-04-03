def extract_OpenAlex(base,params,typeres="req",msg=True):
# -*- coding: utf-8 -*-
    """
    Created on Wed Feb 03 2022
    Fonction permettant l'exploitation de l'API OPENALEX
    Version 4
    
    Args:
        base (str): base à interroger ("works","venues","concepts","authors","institutions")
	    params (dict) : dictionnaire des paramètres (s'appuie sur les paramètres disponibles dans l'API)
        typeres (str) : deux valeurs possibles : "req" (pour éditer la requête) ou "res" (pour obtenir le résultat)
        msg (bool) : pour editer les infos sur la requête        
    
	Returns: 
        Si typeres=="req" : la requête d'interrogation de l'API
        Si typeres=="req" : le dictionnaire contenant les données d'OPENALEX pour les paramètres saisis
        
    @author: davsa
    """
    import requests
    import json
    import copy
    import time as ti
    pref="https://api.openalex.org/"
    params_int=copy.copy(params)
    if "per_page" not in params_int.keys():params_int["per_page"]="200"
    def req_expr(params_int):
        def req_tup(k,v):return (",").join([k + ":" + v if isinstance(v, str) else (",").join([k + ":" + v0 for v0 in v])])        
        return "?"+"&".join([t+"="+(",").join([req_tup(k, v) for k,v in tv.items()]) if isinstance(tv,dict) else t+"="+tv for t,tv in params_int.items()])
    req=pref + base + req_expr(params_int)
    if typeres=="req":
        resret=req
    elif typeres=="res":
        t0=ti.time()
        resret=requests.get(req).json()
        iterval=0
        t1=ti.time()
        nbrec=resret["meta"]["count"]
        if msg is True:
            print("Nombre de lignes ramenées par la requêtes :",str(nbrec))
            print("Durée = %f\n" %(t1-t0))
            print("Durée totale estimée = %f\n" %((t1-t0)*(nbrec/int(params_int["per_page"]))))
        
        if "cursor" in params_int.keys() and params_int["cursor"]=="*":
            resiter=copy.copy(resret)
            while type(resiter['meta']['next_cursor'])== str:
                params_int["cursor"]=resiter['meta']['next_cursor']
                req=pref + base + req_expr(params_int)
                resiter=requests.get(req).json()
                if "group_by" in resiter.keys():resret["group_by"].extend(resiter["group_by"])
                if "group_bys" in resiter.keys():resret["group_bys"].extend(resiter["group_bys"])
                if "results" in resiter.keys():resret["results"].extend(resiter["results"])
                iterval=iterval+1
                print("Boucle : " + str(iterval))
                
    return resret

def inverted_idx_to_str(inverted_idx_txt):
# -*- coding: utf-8 -*-
    """
    Created on Wed Feb 13 2025
    Fonction permettant l'exploitation de transformer l'abstract_inverted_index en texte
    Version 4
    
    Args:
        inverted_idx_txt : abstract_inverted_index d'OpenAlex : dictionnaire de termes et liste de leur position dans le texte
    
	Returns: 
        Un dictionnaire contenant 2 clés : 
            "abstract" : texte de l'abstract reformé
            "missing_terms_list" : liste des termes manquants dans l'index'
        
    @author: jyl, mise à jour davsa
    """
    import re
    new_dico = {}
    for word, idx_list in inverted_idx_txt.items():
        for i in idx_list:
            new_dico[i] = word
    abs = []
    idx_inexist = []
    for i in range(0, max(new_dico.keys())+1):
        try:
            abs.append(new_dico[i])
        except KeyError:
            idx_inexist.append(i)
    abs_str = " ".join(abs)
    return {"abstract":abs_str, "missing_terms_list":idx_inexist}

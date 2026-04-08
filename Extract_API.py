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
    import copy
    import time as ti

    from config_loader import load_config
    from http_client import get_http_session, http_get_json
    from run_logger import RunLogger

    cfg = load_config()
    pref = cfg["openalex"]["api_base_url"]
    http_session = get_http_session()
    run_logger = RunLogger("Extract_API")
    params_int = copy.copy(params)

    if "per_page" not in params_int.keys():
        params_int["per_page"] = cfg["api_extract"]["params"]["per_page"]

    def req_expr(params_loc):
        def req_tup(k, v):
            return (",").join([k + ":" + v if isinstance(v, str) else (",").join([k + ":" + v0 for v0 in v])])

        return "?" + "&".join(
            [
                t + "=" + (",").join([req_tup(k, v) for k, v in tv.items()]) if isinstance(tv, dict) else t + "=" + tv
                for t, tv in params_loc.items()
            ]
        )

    req = pref + base + req_expr(params_int)
    if typeres == "req":
        resret = req
    elif typeres == "res":
        run_logger.info("api_request_start", {"base": base, "params": params_int, "request_url": req})
        t0 = ti.time()
        resret = http_get_json(req, session=http_session)
        iterval = 0
        t1 = ti.time()

        if not isinstance(resret, dict) or "meta" not in resret:
            err = resret.get("message") if isinstance(resret, dict) else str(resret)
            run_logger.error("api_invalid_response", {"request_url": req, "error": str(err)})
            raise RuntimeError("Reponse API invalide pour " + req + ". Message: " + str(err))

        nbrec = resret["meta"]["count"]
        if msg is True:
            print("Nombre de lignes ramenees par la requete :", str(nbrec))
            print("Duree = %f\n" % (t1 - t0))
            print("Duree totale estimee = %f\n" % ((t1 - t0) * (nbrec / int(params_int["per_page"]))))
        run_logger.info(
            "api_first_page_ok",
            {"request_url": req, "count": nbrec, "duration_seconds": round(t1 - t0, 3)},
        )

        if "cursor" in params_int.keys() and params_int["cursor"] == "*":
            resiter = copy.copy(resret)
            while isinstance(resiter["meta"]["next_cursor"], str):
                params_int["cursor"] = resiter["meta"]["next_cursor"]
                req = pref + base + req_expr(params_int)
                resiter = http_get_json(req, session=http_session)
                if not isinstance(resiter, dict) or "meta" not in resiter:
                    err = resiter.get("message") if isinstance(resiter, dict) else str(resiter)
                    run_logger.error("api_invalid_response", {"request_url": req, "error": str(err)})
                    raise RuntimeError("Reponse API invalide pour " + req + ". Message: " + str(err))
                if "group_by" in resiter.keys():
                    resret["group_by"].extend(resiter["group_by"])
                if "group_bys" in resiter.keys():
                    resret["group_bys"].extend(resiter["group_bys"])
                if "results" in resiter.keys():
                    resret["results"].extend(resiter["results"])
                iterval = iterval + 1
                print("Boucle : " + str(iterval))
                run_logger.info("api_cursor_page_ok", {"iteration": iterval, "request_url": req})
        run_logger.info("api_request_end", {"base": base, "total_results": len(resret.get("results", []))})
    else:
        raise ValueError("typeres doit etre 'req' ou 'res'")

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
    new_dico = {}
    for word, idx_list in inverted_idx_txt.items():
        for i in idx_list:
            new_dico[i] = word
    abs_list = []
    idx_inexist = []
    for i in range(0, max(new_dico.keys()) + 1):
        try:
            abs_list.append(new_dico[i])
        except KeyError:
            idx_inexist.append(i)
    abs_str = " ".join(abs_list)
    return {"abstract": abs_str, "missing_terms_list": idx_inexist}

def get_r(elt,val=None,default=None):
    """
    Fonction permettant de lire un élément de tout type et de renvoyer None s'il n'existe pas
    Created on Thu Dec 18 2023

    @author: davsa
    """
    eltout=elt
    if elt is not None and val is not None:
        if not isinstance(elt,dict) and isinstance(val,(list,tuple)):
            val=[len(elt)] if len(val)==0 else val
            val=slice(*val)
        try:eltout=elt[val]
        except (KeyError, IndexError, TypeError):eltout=default
    return eltout

def recurs_list(srclist,key,nb_recurs=1):
    """
    Fonction permettant de lire un élément de liste en appliquant une récursivité dans la profondeur de liste
    Created on Thu Dec 18 2023
    NB : fonction temportaire (pas plus de 5 niveaux possibles)
    => Formule générale à trouver

    @author: davsa
    """
    outlist=[get_r(elt1,key) for elt1 in srclist]
    if nb_recurs==2:
        outlist=[[get_r(elt2,key) for elt2 in elt1] if not set(srclist)=={None} else None for elt1 in srclist]
    if nb_recurs==3:
        outlist=[[[get_r(elt3,key) for elt3 in elt2] for elt2 in elt1] for elt1 in srclist]
    if nb_recurs==4:
        outlist=[[[[get_r(elt4,key) for elt4 in elt3] for elt3 in elt2] for elt2 in elt1] for elt1 in srclist]
    if nb_recurs==5:
        outlist=[[[[[get_r(elt5,key) for elt5 in elt4] for elt4 in elt3] for elt3 in elt2] for elt2 in elt1] for elt1 in srclist]
    return outlist

def flat_list(srclist,reflist,recurs_lev=0,recurs_ref=0):
    """
    Fonction permettant de transformer un élément de liste en appliquant une récursivité en prenant comme référence une liste de niveau 
    Created on Thu Dec 18 2023
    NB : fonction temportaire (pas plus de 5 niveaux possibles)
    => Formule générale à trouver
    
    """
    outlist=srclist
    if recurs_ref==1:
        outlist=[srclist[i] if recurs_lev==recurs_ref else srclist for i,v in enumerate(reflist)]
    if recurs_ref==2:
        outlist=[srclist[i][i1] if recurs_lev==recurs_ref else srclist[i] if isinstance(srclist,list) else srclist for i,v in enumerate(reflist) for i1,v1 in enumerate(v)]
    if recurs_ref==3:
        outlist=[srclist[i][i1][i2] if recurs_lev==recurs_ref else srclist[i][i1] if isinstance(srclist[i],list) else srclist[i] if isinstance(srclist,list) else srclist for i,v in enumerate(reflist) for i1,v1 in enumerate(v) for i2,v2 in enumerate(v1)]
    if recurs_ref==4:
        outlist=[srclist[i][i1][i2][i3] if recurs_lev==recurs_ref else srclist[i][i1][i2] if isinstance(srclist[i][i1],list) else srclist[i][i1] if isinstance(srclist[i],list) else srclist[i] if isinstance(srclist,list) else srclist for i,v in enumerate(reflist) for i1,v1 in enumerate(v) for i2,v2 in enumerate(v1) for i3,v3 in enumerate(v2)]
    if recurs_ref==5:
        outlist=[srclist[i][i1][i2][i3][i4] if recurs_lev==recurs_ref else srclist[i][i1][i2][i3] if isinstance(srclist[i][i1][i2],list) else srclist[i][i1][i2] if isinstance(srclist[i][i1],list) else srclist[i][i1] if isinstance(srclist[i],list) else srclist[i] if isinstance(srclist,list) else srclist for i,v in enumerate(reflist) for i1,v1 in enumerate(v) for i2,v2 in enumerate(v1) for i3,v3 in enumerate(v2) for i4,v4 in enumerate(v3)]
    return outlist

def get_elt_path(dct,keylist=None):
    """
    Fonction permettant de lire un élément en fonction de son chemin dans un dictionnaire complexe / multi-niveau
    Created on Thu Dec 18 2023
    => Formule générale à trouver

    @author: davsa
    """
    kv=dct
    if keylist is not None:
        keylist=[keylist] if not isinstance(keylist,(list,tuple)) else keylist
        k_1=None
        recur=0
        for i,k in enumerate(keylist):
            if isinstance(kv,(tuple,list)) and isinstance(k_1,(tuple,list)):
                recur+=1
                kv=recurs_list(kv,k,nb_recurs=recur)
            elif recur>0:
                kv=recurs_list(kv,k,nb_recurs=1) # A vérifier si plus de 2 niveaux : soit "nb_recurs=1", soit "nb_recurs=recur-1"
            else:kv=get_r(kv,k)
            k_1=k                
        kv=[] if kv is None and [isinstance(kl,list) for kl in keylist] else kv
    if isinstance(kv,(tuple,list)) and (sum([len(str(g)) for g in kv if g is not None])==0 or kv==[[]]*len(kv)):kv=[["Inconnu"]]*len(kv)
    return kv

def trs_form(val):
    """
    Fonction permettant de définir le champ de formats en vue d'une intégration SQL
    Args
        :val:valeur de format 

    Returns
        Chaîne de caractères de définition du format
    """
    trs_dct={"str":"VARCHAR2","num":"NUMBER","float":"FLOAT","bool":"NUMBER","clob":"CLOB"}
    val=val.split("_")
    typc=" CHAR" if val[0]=="str" else ""
    typc="(" + val[1] + typc + ")" if len(val)>1 else ""
    typc=" " + trs_dct[val[0]] + typc
    return typc

def lstv_form(lst_paths,addnfic=True):
    nf=["NFIC NUMBER"] if addnfic is True else []
    return nf + [k + trs_form(v["form"]) for k,v in lst_paths.items()]

def net_val(val,left=0,right=None):
    return val[left:right] if isinstance(val,str) else int(val) if isinstance(val,bool) else None if val==[] else ",".join(val) if isinstance(val,(tuple,list)) else val

def trt_rec_old(rec,lst_paths,pref,lims):
    """
    Fonction permettant de traiter la lecture d'un enregistrement JSON
    Args
        :rec:Enregistrement JSON à traiter
        :lst_paths:dictionnaire de références des variables souhaités et des "paths" correspondants dans le fichier
        :pref:dictionnaire des préfixes (troncatures à gauche)
        :lims:dictionnaire limites de champs (troncatures à droite)
    Returns
        dictionnaire contenant les items d'intérêt tels de définis dans lst_paths
    """
    import json
    net={l:len(pref[l]) if l in pref.keys() else 0 for l in lst_paths.keys()}
    lim={l:lims[l] if l in lims.keys() else None for l in lst_paths.keys()}
    j=json.loads(rec.decode('utf-8'))
    jsl={k:get_elt_path(j,v["path"],net[k],lim[k]) for k,v in lst_paths.items()}
    return jsl  

def trt_rec(rec,lst_paths,pref,lims,jsonfile=True):
    """
    Fonction permettant de traiter la lecture d'un enregistrement JSON
    Args
        :rec:Enregistrement JSON à traiter
        :lst_paths:dictionnaire de références des variables souhaités et des "paths" correspondants dans le fichier
        :pref:dictionnaire des préfixes (troncatures à gauche)
        :lims:dictionnaire limites de champs (troncatures à droite)
        :jsonfile:
    Returns
        dictionnaire contenant les items d'intérêt tels de définis dans lst_paths
    """
    import json
    net={l:len(pref[l]) if l in pref.keys() else 0 for l in lst_paths.keys()}
    lim={l:lims[l] if l in lims.keys() else None for l in lst_paths.keys()}
    recurs_lev={k:sum([isinstance(p,(list,tuple)) for p in v["path"]]) for k,v in lst_paths.items()}
    recurs_ref=max(recurs_lev.values())
    refkey=[i for i,r in recurs_lev.items() if r==recurs_ref][0]
    j=json.loads(rec.decode('utf-8')) if jsonfile is True else rec
    jsl={k:get_elt_path(j,v["path"]) for k,v in lst_paths.items()}
    #with open('data_err.txt', 'w') as f:
    #    f.write(str(jsl))
    if recurs_ref==0:
        jsl=[{k:net_val(v,net[k],lim[k]) for k,v in jsl.items()}]
    else:
        jsl={k:flat_list(jsl[k],jsl[refkey],recurs_lev=recurs_lev[k],recurs_ref=recurs_ref) for k in jsl.keys()}
        jsl=[{k:net_val(jsl[k][i],net[k],lim[k]) for k in jsl.keys()} for i in range(len(jsl[refkey]))]
    jsl=[{k:(None if v=="Inconnu" else v) for k,v in js.items()} for js in jsl]
    return jsl  

def read_gz(dir,fname):
    """
    Import et lecture Buffer d'une archive gzip contenant un seul fichier 
    """
    import gzip
    import io
    fn=dir + "\\"  + fname["nomfic"] + ".gz"
    gz=gzip.open(fn, 'rb')
    return io.BufferedReader(gz)

def sub_list(lst,nb=0,nelts=0):
    """
    Découpage d'une liste en sous-listes de même taille
    Args
        :lst:liste à découper
        :nb:nombre de sous-listes souhaitées
        :nelts:nombre d'éléments par liste
    
    Returns
        liste d'entrée divisée en sous-listes 
    """
    if nb>0 and nelts>0:print("Définir le nombre de sous-listes ou le nombre d'éléments par sous-liste, pas les deux")
    else:
        if nb==0 and nelts==0:outlist=lst
        else:
            if nb>0:pas=int(len(lst)/nb) if int(len(lst)/nb)==len(lst)/nb else int(len(lst)/nb)+1
            else:pas=nelts
            outlist=[lst[i:i+pas] for i in range(0,len(lst),pas)]
        return outlist

def create_tables(table,lst_paths,tabref="",connex=""):
    import sys
    from config_loader import load_config
    cfg=load_config()
    sys.path.append(cfg["oracle"]["embedded_sql_src_path"])
    import sql_admin as sa
    sa.sql_create(table,"l",lstv_form(lst_paths),connex=connex,run=True)
    if not tabref=="":
        sa.sql_create(tabref,"l",["nfic NUMBER","NOMFIC VARCHAR2(30 CHAR)","NBREC NUMBER","DUREE_IMPORT FLOAT"],connex=connex,run=True)

def create_index(table,connex=""):
    import sys
    from config_loader import load_config
    cfg=load_config()
    sys.path.append(cfg["oracle"]["embedded_sql_src_path"])
    import sql_admin as sa
    sa.sql_index(table,"01",["ID"],connex=connex,run=True)

def sql_modif_clob(table,col,connex=""):
    c=connex.cursor()
    c.execute(("Create Or Replace Function clob_substr(p_clob In Clob,p_offset In Pls_Integer,p_length In Pls_Integer) Return Varchar2 Is"
    "\n Begin Return substrb(dbms_lob.substr(p_clob,p_length,p_offset),1,p_length); End;"))                            
    c.execute("ALTER TABLE " + table + " RENAME COLUMN " + col + " TO " + col + "_CLOB")
    c.execute("ALTER TABLE " + table + " ADD " + col + " VARCHAR2(4000 CHAR)")
    c.execute("UPDATE " + table + " SET " + col + "=clob_substr(" + col + "_CLOB,1,4000)")
    c.execute("ALTER TABLE " + table + " DROP COLUMN " + col + "_CLOB")
    connex.commit()


def trt_fic(fname,io_gz,connex="",table="",tabref="",lst_paths="",pref="",lims="",setids=""):
    import time as ti
    lstvars=["NFIC"] + list(lst_paths.keys())
    reqv="INSERT INTO " + table + " VALUES (" + ",".join([":"+ k for k in lstvars]) + ")"
    reqvref="INSERT INTO " + tabref + " VALUES (:nfic,:nomfic,:nbrec,:duree_import)"

    print("Fichier n° " + str(fname["nfic"]) + " : " + fname["nomfic"])
    c=connex.cursor()
    t0=ti.time()
    c.execute("DELETE FROM " + table + " WHERE nfic=" + str(fname["nfic"]))
    c.execute("DELETE FROM " + tabref + " WHERE nfic=" + str(fname["nfic"]))
    connex.commit()

    nfic={"nfic":fname["nfic"]}

    i=0
    i1=0
    for l in io_gz:
      #if i1<1000:
      #  print(i1)
        #if i1==27650:print("#"*30 + str(i1) + "#"*30)
        #print("#"*30 + " : " + str(i1) + "#"*30)
        #print(l)
        i2=0
        #if i1==27650:
        trts=trt_rec(l,lst_paths,pref,lims)
        if not setids=="":
            trts=[tr for tr in trts if tr["id"] in setids]
        for t in trts:
            #print(reqv)
            #if i/10000==int(i/10000):
            #    print("Valeur de i : " + str(i))
                #print(t)
            #with open('data_err.txt', 'w') as f:
            #    f.write(t["id"] + t["author_id"])
            c.execute(reqv,{**nfic,**t})
            i2+=1
            i+=1
        i1+=1        
    fname["nbrec"]=i
    t1=ti.time()   
    fname["duree_import"]=t1-t0
    c.execute(reqvref,fname)
    connex.commit()
    print("Dézippage du fichier " + fname["nomfic"] + ".gz, lecture et transfert SQL : Durée = %f\n" %(t1-t0))

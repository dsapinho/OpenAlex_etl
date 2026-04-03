# -*- coding: utf-8 -*-
import sys
import cx_Oracle as cx
def sql_drop(table,connex="",prt=False,run=False,msg=True,log="",titreq="",nbc="-"*120,scn=""):
    # -*- coding: utf-8 -*-
    """Fonction permettant de supprimer une table en gérant l'exception de non existence de la table

    Created on Thu Feb 25 11:26:50 2021

    @author: davsa

    Params:
        :table (str): nom de la table à supprimer
        :connex (connecteur): nom du connecteur cx_Oracle (défaut : "")
        :prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête
        :run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex"
        :msg (booléenne): si True, des messages de log sont écrits

    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur

    """
    import datetime as dt
    import time as ti
    import cx_Oracle as cx

    txt_sql=("drop table " + table + " PURGE")
        
    if log=="":
        print("Le paramètre log doit contenir un nom de fichier")
    else:
        logf=open(log, "a")
        
        expt=False

        #msgnreq="Requête n°" + str(nreq) if nreq>=0 else ""
        if 'nreq' in globals():
            global nreq
            nreq=nreq+1
            msgnreq="Requête n°" + str(nreq) 
        else:
            msgnreq=""

        titreq=" : " + titreq if titreq!="" else ""

        txt_log=("\n" + nbc + 
                "\n" + msgnreq + titreq + 
                "\nDate et Heure de début : " + dt.datetime.today().strftime('%d/%m/%y %H:%M:%S') + 
                "\n" + nbc + 
                "\n" + txt_sql)
        t1=ti.time()
    
    if run==True and str(type(connex))!="<class 'cx_Oracle.Connection'>":
        print("Le paramètre connex doit être de type <class 'cx_Oracle.Connection'>")
    else:
        if run==True:
            try:
                c=connex.cursor()
                c.execute(txt_sql)
                connex.commit()
                c.close()
            except connex.DatabaseError:
                if not log=="":txt_log=txt_log + "\n La table " + table + " n'existait pas"
                if msg==True:print("La table " + table + " n'existait pas")
            else:
                if not log=="":txt_log=txt_log + "\n La table " + table + " a été supprimée"
                if msg==True:print("La table " + table + " a été supprimée")
        elif prt==True:
            if not log=="":txt_log=(txt_log +"\n" + nbc + "\nEdition du code uniquement")
            if msg==True:print("Edition du code de la requête drop table " + table )        
        if not log=="":
            t2=ti.time()
            t=str(dt.timedelta(seconds=t2-t1)).split(".")[0].split(":")    
            txt_log=(txt_log + 
                    "\n" + nbc + 
                    "\nDate et Heure de fin : " + dt.datetime.today().strftime('%d/%m/%y %H:%M:%S') +
                    "\nDurée du traitement : " + t[0] + " hrs " + t[1] + " min " + t[2] + " sec " + 
                    "\n" + nbc)
            logf.write(txt_log)
            logf.close()

def init_log(log="",titreq="",comment="",instance="",schema="",nbc="-"*120):
    # -*- coding: utf-8 -*-
    """Fonction permettant d'initialiser un fichier de log d'execution des requetes
    
    Created on Nov 29 2022
    
    @author: davsa
        
    Args:
        log (str): Nom du fichier de logs
        
        titreq (str): Titre de la requête
        
        instance (str): Nom de l'instance Oracle où s'exécutent la/les requête(s)
        
        schema (str): Nom du schema depuis lequel la/les requête(s) est/sont lancée(s) 
        
        nbc (str): Chaine de caractère délimitant le texte du log

    Returns:
        Heure de lancement du script

    """

    import datetime as dt
    import time as ti
    global time_init
    time_init=ti.time()
    global nreq
    nreq=0

    if log=="":
        print("Le paramètre log doit contenir un nom de fichier")
    
    else:
        logf=open(log, "w", encoding="utf-8")
        logf.write(nbc)
        logf.write("\nDébut d'exécution du script : " + titreq)
        comment="\n" + nbc + "\n" + comment if comment!="" else comment
        logf.write(comment)
        logf.write("\nInstance     : " + instance + ",\nSchéma     : " + schema + 
                  "\nDate et Heure de début : " + dt.datetime.today().strftime('%d/%m/%y %H:%M:%S'))
        logf.write("\n" + nbc + "\n")
    #return ti.time()

def sql_print_log_except(txt_sql,table,connex="",drop=False,prt=True,run=False,msg=False,log="",titreq="",nbc="-"*120):
    # -*- coding: utf-8 -*-
    """Fonction permettant d'éditer le résultat d'une requête dans un fichier de log
    
    Created on Nov 29 2022
    
    @author: davsa
        
    Args:
        txt_sql (str): Script de la requête
        
        table (str):nom de la table à créer entre ""
        
        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")
        
        drop (booléenne) : Si True, supprime préalablement la table sur laquelle s'applique le traitement (défaut : False)
        
        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")
        
        titreq (str): Titre de la requête (défaut : "")
        
        nbc (str) : Chaine de caractère délimitant le texte du log (défaut : "-"*120)
        
        nreq (int) : Numéro de requête (défaut : -1)
    
    Returns:
        Pas de valeur retournée 
    """
    import datetime as dt
    import time as ti
    import cx_Oracle as cx
    if log=="":
        print("Le paramètre log doit contenir un nom de fichier")
    else:
        logf=open(log, "a", encoding="utf-8")
        
        expt=False

        #msgnreq="Requête n°" + str(nreq) if nreq>=0 else ""
        if 'nreq' in globals():
            global nreq
            nreq=nreq+1
            msgnreq="Requête n°" + str(nreq) 
        else:
            msgnreq=""

        titreq=" : " + titreq if titreq!="" else ""
        
        txt_log=("\n" + nbc + 
                "\n" + msgnreq + titreq + 
                "\nDate et Heure de début : " + dt.datetime.today().strftime('%d/%m/%y %H:%M:%S') + 
                "\n" + nbc + 
                "\n" + txt_sql)
        t1=ti.time()

        if run==True and str(type(connex))!="<class 'cx_Oracle.Connection'>":
            txt_log=(txt_log + 
                    "\n" + "Le paramètre connex doit être de type <class 'cx_Oracle.Connection'>")
        elif run==True:
            if drop==True:
                sql_drop(table,connex,run=run,msg=msg)
            if msg==True:print(titreq)
            try:
                c=connex.cursor()
                c.execute(txt_sql)
            except cx.DatabaseError as ex:
                error = ex.args[0]        
                pos_error=txt_sql[error.offset:].split(" ")[0]
                txt_log=(txt_log + 
                        "\n" + nbc + 
                        "\nERREUR ORACLE"
                        "\n" + error.message + 
                        "\nElément de la requête concerné :\n" + pos_error + 
                        "\nLE SCRIPT EST INTERROMPU")        
                connex.rollback()
                #raise
                expt=True
            else:
                txt_log=(txt_log + "\n\nLa requête s'est exécutée avec succès")        
                connex.commit()
            finally:
                c.close()
        elif prt==True:
            txt_log=(txt_log + 
                    "\n" + nbc + "\nEdition du code uniquement")

        t2=ti.time()
        t=str(dt.timedelta(seconds=t2-t1)).split(".")[0].split(":")    
        txt_log=(txt_log + 
                "\n" + nbc + 
                "\nDate et Heure de fin : " + dt.datetime.today().strftime('%d/%m/%y %H:%M:%S') +
                "\nDurée du traitement : " + t[0] + " hrs " + t[1] + " min " + t[2] + " sec " + 
                "\n" + nbc)
        logf.write(txt_log)
        logf.close()
        if expt is True:
            print("Erreur Oracle : script interrompu")
            exit()

def close_log(log="",time_init=0,nbc="-"*120):
    # -*- coding: utf-8 -*-
    """Fonction permettant de cloturer un fichier de log d'execution des requetes
    Created on Nov 29 2022

    @author: davsa
        
    Args:
        log (str): Nom du fichier de logs
        
        time_init (float): Valeur de temps
        
        nbc (str) : Chaine de caractère délimitant le texte du log (défaut : "-"*120)
        
        nreq (int) : Numéro de requête (défaut : -1)
    
    Returns:
        Pas de valeur retournée 
	
    """
    import datetime as dt
    import time as ti
    time_end=ti.time()
    if log=="":
        print("Le paramètre log doit contenir un nom de fichier")
    else:
        logf=open(log, "a", encoding="utf-8")    
        
        logf.write("\n" + nbc)
        logf.write("\nFin d'exécution du script")
        logf.write("\nL'ensemble des requêtes s'est exécuté avec succès")
        if 'nreq' in globals():
            logf.write("\nNombre de requêtes : " + str(nreq))
        
        t=str(dt.timedelta(seconds=time_end-time_init)).split(".")[0].split(":")
        
        dur_trt="Non calculée (Heure au début de l'exécution non précisée)" if time_init==0 else t[0] + " hrs " + t[1] + " min " + t[2] + " sec "
        
        logf.write("\nDate et Heure de fin : " + dt.datetime.today().strftime('%d/%m/%y %H:%M:%S') +
                  "\nDurée totale du traitement : " + dur_trt )
        logf.write("\n" + nbc)
        logf.close()

def sql_edit(txt_sql,table,connex="",drop=False,typreq="",prt=False,run=False,msg=True,log="",scn=""):
    # -*- coding: utf-8 -*-
    """Fonction permettant l'édition d'un code sql (return) ou l'exécution sous Oracle, en intégrant l'édition du log
    Created on 17/02/2022

    @author: davsa
        
    Args:
        txt_sql (str): script SQL à éditer ou à exécuter
        
        table (str):nom de la table à créer entre ""
        
        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")
        
        drop (booléenne) : Si True, supprime préalablement la table sur laquelle s'applique le traitement (défaut : False)

        typreq (str):type de requête (5 pris en charge : "CREATE","INSERT","UPDATE","DELETE","INDEX","PK").
        
        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité
                          
    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur

    """

    t=typreq.upper()
    msgs={"CREATE":{"pr":"CREATE TABLE ","ex":"La table " + table + " a été crée","log":"Création de la table " + table}
         ,"CREATE_VW":{"pr":"CREATE OR REPLACE VIEW ","ex":"La vue " + table + " a été crée","log":"Création de la vue " + table}
         ,"INSERT":{"pr":"INSERT INTO ","ex":"Les données ont été insérées dans la table " + table,"log":"Insertion de données dans la table " + table}
         ,"UPDATE":{"pr":"UPDATE ","ex":"Les données de la table " + table + "ont été mises à jour","log":"Mise à jour de données dans la table " + table}
         ,"DELETE":{"pr":"DELETE FROM ","ex":"Les données ont été supprimées de la table " + table, "log":"Suppression de données dans la table " + table}
         ,"INDEX":{"pr":"CREATE INDEX ","ex":"L'index a été ajouté à la table " + table, "log":"Ajout d'un index à la table " + table}
         ,"PK":{"pr" : "ALTER TABLE ADD PRIMARY KEY ","ex": "Une clé primaire a été ajoutée sur la table " + table, "log":"Ajout d'une clé primaire à la table " + table}
         ,"GRANT":{"pr" : "GRANT ON ","ex" : "Les droits ont été donnés sur la table " + table,"log":"Ajout de droits à la table " + table}
        }
    
    if run==True and str(type(connex))!="<class 'cx_Oracle.Connection'>":
        print("Le paramètre connex doit être de type <class 'cx_Oracle.Connection'>")
    elif t not in msgs.keys():
        print("Seules les opérations de type " + ",".join(msgs.keys()) + " peuvent être réalisées")
    else:
        if t=="CREATE" and drop==False:
           print("La table " + table + " est créée sans suppression préalable (mettre drop=True si nécessaire)")
        
        if not (log==""):
           scn="\nNom du script contenant la requête : " + scn if not scn=="" else ""
           tit=msgs[t]["log"] + scn
           sql_print_log_except(txt_sql,table,connex=connex,drop=drop,run=run,msg=msg,log=log,titreq=tit,nbc="-"*120)            
        
        else:
            if run==True:
                if drop==True:
                    sql_drop(table,connex=connex,run=run,msg=msg)
                try:
                    c=connex.cursor()
                    if t=="INSERT":
                        c.execute("ALTER SESSION ENABLE PARALLEL DML")
                    c.execute(txt_sql)
                except cx.DatabaseError as ex:
                    if msg==True:print("Erreur Oracle : script interrompu")
                    connex.rollback()
                else:
                    connex.commit()
                    if msg==True:print(msgs[t]["ex"])
                finally:
                    c.close()
                return ""
            if prt==True:
                if msg==True:print("Edition du code de la requête " + msgs[t]["pr"] + table )
                return txt_sql 

def sql_create(table,typchamps,defchamps,connex="",partchamp="",partminval="",partmaxval="",tablespace=""
               ,reqoptions="NOCACHE NOLOGGING COMPRESS BASIC PARALLEL",prt=False,run=False,msg=True,log="",scn=""):
    # -*- coding: utf-8 -*-
    """Fonction permettant de créer une table vide à partir d'une définition de champs (liste python ou fichier csv)
    Created on Thu Feb 25 11:26:50 2021

    @author: davsa
        
    Args:
        table:nom de la table à créer entre "".
        
        typchamps:type d'objet contenant la création des champs ("l"=liste ou "f"=fichier).
        
        defchamps: si typchamps = "l", une liste python des champs et leurs types ou attributs ou la ref d'un fichier de définition des champs.

        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        partchamp:Champ sur lequel partitionner les données (par défaut, vide).

        partminval:Valeur minimale de partitionnement (par défaut, vide).

        partmaxval:Valeur maximale de partitionnement (par défaut, vide).

        tablespace:tablespace auquel appliquer le traitement (par défaut, vide).

        reqoptions:options de la requête (par défaut, NOCACHE NOLOGGING COMPRESS BASIC PARALLEL).

        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité
                          
    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur
                          	
    """
    # Définition des champs
    returnval=""
    if not typchamps in ["l","f"]:
        print('Le paramètre typchamps doit prendre la valeur "l"(liste) ou "f"(fichier)')
    else:
        if typchamps=="f":
            champs = open((defchamps + ".txt"), "r")
            lstchamps=champs.read().replace('\n','\n,')
        elif typchamps == "l":
            lstchamps=",\n".join(defchamps)
            
        txt_sql0_crea=("CREATE TABLE " + table + 
                    " (\n" + lstchamps + "\n)")                 
        if partchamp != "":               
            nbpartval=partmaxval-partminval+1
            txt_sql0_crea=(txt_sql0_crea + "\nPARTITION BY RANGE(" + partchamp + ")\n(")
            for i in range(partmaxval+1)[-nbpartval+1:]:
                s="\n,"
                si=str(i)
                txt_sql0_crea=(txt_sql0_crea + 
                            "PARTITION p" + si + table + " VALUES LESS THAN (" + si + ")" + s)
            txt_sql0_crea=txt_sql0_crea + "\nPARTITION pAutr" + table + " VALUES LESS THAN (MAXVALUE))"
            
        if tablespace != "":tablespace="TABLESPACE " + tablespace
        
        txt_sql0_crea=(txt_sql0_crea + "\n " + tablespace + "\n " + reqoptions )
            
        returnval=sql_edit(txt_sql0_crea,table,connex=connex,typreq="CREATE",drop=True,prt=prt,run=run,msg=msg,log=log,scn=scn)
    return returnval

def sql_insert(table,typchamps,defchamps,src_data,connex="",typreq="INSERT",typins="SELECT",parallel="",distinct=False,filtre="",gpbychamps="",ordbychamps=""
               ,prt=False,run=False,msg=True,log="",scn=""):
    # -*- coding: utf-8 -*-
    """Fonction permettant d'insérer des données dans une table, soit en la créant, soit en les insérant dans une table existante
    Created on Thu Feb 25 11:26:50 2021

    @author: davsa
        
    Args:
        table: nom de la table à créer entre "".
        
        typchamps: type d'objet contenant la création des champs ("l"=liste ou "f"=fichier).
        
        defchamps: si typchamps = "l", une liste python des champs et leurs types ou attributs ou la ref d'un fichier de définition des champs.

        src_data: table source ou liste de valeurs à insérer.

        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        typreq: type de requête ("CREATE"=CREATE XXX AS ou "INSERT"=INSERT INTO XXX)

        typins: Type d'insertion, "SELECT" ou "VALUES" (uniquement avec typreq="INSERT")

        distinct: si vrai, application d'un DISTINCT aux valeurs

        filtre: selection de données (clause where).

        gpbychamps: liste des champs du group by.

        odbychamps: liste des champs de l'order by.

        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité
                          
    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur
	
    """
    flt="\n WHERE " + filtre if filtre !="" else ""

    prl="/*+ PARALLEL(" + parallel + ") */" if not parallel=="" else ""
    
    dst="DISTINCT " if distinct else ""
    
    gpby="\n GROUP BY " + ",\n".join(gpbychamps) if gpbychamps !="" else ""
    
    odby="\n ORDER BY " + ",\n".join(ordbychamps) if ordbychamps !="" else ""

    drop=False

    returnval=""

    if not typchamps in ["l","f"]:
        print('Le paramètre typchamps doit prendre la valeur "l"(liste) ou "f"(fichier)')
    elif not typreq in ["CREATE","CREATE_VW","INSERT"]:
        print("Pour une insertion de données, le paramètre typreq doit être 'CREATE','CREATE_VW' ou 'INSERT'")
    elif typreq in ["CREATE","CREATE_VW"] and typins=="VALUES":
        print("Il n'est pas possible de créer (CREATE) une table à partir d'une liste de valeurs"
              "\n utilisez d'abord sql_create puis sql_insert")
    else:
        if typchamps=="f":
            champs = open((defchamps + ".txt"), "r")
            lstchamps=champs.read().replace('\n','\n,')
        elif typchamps == "l":
            lstchamps=",\n".join(defchamps)

        if typins=="SELECT":
            add_vals=("\n SELECT " + prl + dst + lstchamps +
                    "\n FROM " + src_data + flt + gpby + odby)
    
        if typins=="VALUES":
            add_vals="\n VALUES (" + lstchamps + ")"

        if typreq=="INSERT":
            txt_sql0_ins="INSERT INTO " + table + add_vals
        elif typreq=="CREATE":
            drop=True
            txt_sql0_ins="CREATE TABLE " + table + " AS " + add_vals
        elif typreq=="CREATE_VW":
            txt_sql0_ins="CREATE OR REPLACE VIEW " + table + " AS " + add_vals

        returnval=sql_edit(txt_sql0_ins,table,connex=connex,typreq=typreq,drop=drop,prt=prt,run=run,msg=msg,log=log,scn=scn)    
    return returnval

def sql_pk(table,lstchamps,connex="",prt=False,run=False,msg=True,log="",scn=""):
    """Fonction permettant de créer une clé primaire sur les champs d'une table
    Created on Thu Feb 25 11:26:50 2021

    @author: davsa
        
    Args:
        table: nom de la table à laquelle associer une clé primaire.
        
        lstchamps: liste des champs de la clé primaire.
        
        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité
                          
    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur
	
    """
    returnval=""
    txt_sql=("ALTER TABLE " + table + " ADD CONSTRAINT pk_" + table + 
    "\n PRIMARY KEY (" + ",".join(lstchamps) + ")")
    
    returnval=sql_edit(txt_sql,table,connex=connex,typreq="PK",prt=prt,run=run,msg=msg,log=log,scn=scn)
    return returnval
   
    
def sql_index(table,num_index,lstchamps,connex="",unique=False,bitmap=False,tablespace=""
               ,reqoptions="NOLOGGING PARALLEL",prt=False,run=False,msg=True,log="",scn=""):
    # -*- coding: utf-8 -*-
    """Fonction permettant de créer un index sur les champs d'une table
    Created on Thu Feb 25 11:26:50 2021

    @author: davsa
        
    Args:
        table:nom de la table à laquelle appliquer l'index entre "".
        
        num_index:numéro/indice de l'index .
        
        lstchamps:liste des champs de l'index.
        
        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        unique (booléenne): Si True, alors index unique, si False, index non unique (défaut : False)
        
        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité
                          
    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur
	
    """
    u="UNIQUE " if unique else ""
    b="BITMAP " if bitmap else ""
    tablespace="TABLESPACE " + tablespace if tablespace != "" else ""

    returnval=""
    txt_sql_index=("CREATE " + u + b + "INDEX i" + num_index + table + 
                   "\n ON " + table + "(\n" + "\n,".join(lstchamps) +"\n)" + 
                    "\n " + tablespace + "\n " + reqoptions)
    returnval=sql_edit(txt_sql_index,table,connex=connex,typreq="INDEX",drop=False,prt=prt,run=run,msg=msg,log=log,scn=scn)

    return returnval

def sql_grant(table,user,connex="",typgrant="SELECT",prt=False,run=False,msg=True,log="",scn=""):
    """Fonction permettant de donner des droits sur une table 
    
    Created on Thu Feb 25 11:26:50 2021
    
    @author: davsa
        
    Args:
        table:nom de la table à laquelle appliquer l'index entre "".
        
        user:nom du schéma à qui attribuer les grants
        
        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        typgrant:type de grants (défaut : "SELECT")
        
        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité
                          
    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur
	
    """

    returnval=""
    txt_sql_grant=("GRANT " + typgrant + " ON " + table + " to " + user)
    returnval=sql_edit(txt_sql_grant,table,connex=connex,typreq="GRANT",drop=False,prt=prt,run=run,msg=msg,log=log,scn=scn)

    return returnval

def sql_grant_schema(user,connex="",typgrant="SELECT",prt=False,run=False,msg=True,log="",scn=""):
    """Fonction permettant de donner des droits sur l'ensemble des tables d'un schéma 
    
    Created on Thu Feb 25 11:26:50 2021
    
    @author: davsa
        
    Args:
        user:nom du schéma à qui attribuer les grants
        
        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        typgrant:type de grants (défaut : "SELECT")
        
        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité

    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur
    """
    txt_sql="select distinct segment_name from user_segments where segment_type in ('TABLE','TABLE PARTITION')"
    c=connex.cursor()
    tab_list=c.execute(txt_sql)
    for table in tab_list:
        sql_grant(table[0], user, connex=connex,prt=prt,run=run,msg=msg,log=log,scn=scn)


def sql_update(table,cols,colval,connex="",filtre="",prt=False,run=False,msg=True,log="",scn=""):
    """Fonction permettant de mettre à jour les données d'une table 
    
    Created on Thu Feb 25 11:26:50 2021
    
    @author: davsa
        
    Args:
        table: nom de la table à laquelle appliquer l'UPDATE entre "".

        cols: nom des champs à mettre à jour

        colval: valeurs à affecter aux colonnes

        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité

    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur

    """
    returnval=""
    filtre="\n WHERE " + filtre if not filtre=="" else ""
    txt_sql_update=("UPDATE " + table + " SET (" + cols + ") = (" + colval + ")" + filtre)
    returnval=sql_edit(txt_sql_update,table,connex=connex,typreq="UPDATE",drop=False,prt=prt,run=run,msg=msg,log=log,scn=scn)

    return returnval

def sql_delete(table,connex="",filtre="",prt=False,run=False,msg=True,log="",scn=""):
    """Fonction permettant de supprimer les données d'une table 
    
    Created on Thu Feb 25 11:26:50 2021
    
    @author: davsa
        
    Args:
        table: nom de la table à laquelle appliquer la suppression entre ""

        connex (connecteur): nom du connecteur cx_Oracle (défaut : "")

        prt (booléenne): si True, la fonction renvoie la chaîne de caractères de la requête (défaut : False)
        
        run (booléenne): si True, la fonction exécute la requête dans Oracle, schéma "connex" (défaut : False)
        
        msg (booléenne): si True, des messages de log sont écrits (défaut : False)
        
        log (str): Nom du fichier de logs (défaut : "")

        scn: Nom du script contenant le code édité

    Returns:
        Si prt==True : Code SQL de la requête
        
        Si run==True : Execute la requête dans le schéma Oracle pointé par le connecteur

    """

    returnval=""
    filtre="\n WHERE " + filtre if not filtre=="" else ""
    txt_sql_update=("DELETE FROM " + table + filtre)
    returnval=sql_edit(txt_sql_update,table,connex=connex,typreq="DELETE",drop=False,prt=prt,run=run,msg=msg,log=log,scn=scn)

    return returnval

def sql_ech_alea(table,tab_src,id,connex="",nb=100,prt=False,run=False,msg=True,log="",scn=""):
    tab_ech="(select a.*,rownum as n from (select " + id + " from " + tab_src + " order by dbms_random.value()) a)"
    flt="n<=" + str(nb)
    sql_insert(table,"l",[id],tab_ech,connex=connex,typreq="CREATE",filtre=flt,prt=prt,run=run,msg=msg,log=log,scn=scn)


# Récupération du fichier contenant la liste des fichiers du SNAPSHOT
def cp_url(url,file="",mode="w",copy=False,ret=False,prt=False):
    """
    Fonction de copie d'un fichier du SNAPSHOT dans un dossier local
    """
    import requests
    import time as ti
    t0=ti.time()
    resp = requests.get(url)
    fout=resp.content if mode=="wb" else resp.text if mode=="w" else None
    if copy is True:
        with open(file,mode=mode) as f:
            f.write(fout)
    if ret is True:
        return resp.json()

    t1=ti.time()
    if prt==True:print("Copie locale du fichier " + file + " : Durée = %f\n" %(t1-t0))

url="https://openalex.s3.amazonaws.com/"

# Répertoire local des fichiers
path="XXX"

import time as ti
# Liste des types de données à télécharger
lst_data=["works","authors","institutions","topics","fields","subfields","concepts","domains","funders","publishers","sources"]

lstf={"files":[]}

t0=ti.strftime('%H:%M:%S', ti.gmtime())
with open("load_files.log","w") as f:
    f.write(f"Heure de lancement : {t0}\n")
f.close()

for l in lst_data:
    lst_load=cp_url(url + "data/" + l + "/manifest",path + l + "_manifest",ret=True)["entries"]
    for load in lst_load:
        url_file=load["url"].replace("s3://openalex/",url)
        lf=load["url"].split("/")
        ln=lf[4]+"_" + lf[5][13:].replace("-","") + "_" + lf[6]
        local_file=path + "files/" + ln 
        t0=ti.time()
        cp_url(url_file,local_file,mode="wb",copy=True)
        t1=ti.time()
        with open("load_files.log","a") as f:f.write(str({**{"file":ln[:-3]},**load["meta"],**{"duree":round(t1-t0,3)}})+"\n")
        print(url_file + " => " + local_file)

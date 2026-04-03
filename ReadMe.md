# OpenAlex_etl
Projet d'intégration des données d'OpenAlex dans un SI Local

## 🌟 Fonctionnalités Clés
- **Extraction** : Extraction automatisée de données venant de fichiers ".json" (SNAPSHOT ou API Online)
- **Stockage** : Stockage dans Oracle ou MongoDB.

## 🛠 Pré-requis
- **Python**
- **Oracle**
- **MongoDB**

## Scripts disponibles
- **Extract_API.py** : Fonctions d'extraction de données à partir de l'API OpenAlex
- **OpenAlex_GetSnap_20241231.py** : Chargement du SNAPSHOT OpenAlex dans un SI local
- **OpenAlex_ReadApi_wks_grants_ERC.py** : Lecture de données de grants OpenAlex à partir de l'API
- **OpenAlex_ReadSnap_wks_pub.py** : Lecture de données OpenAlex à partir du SNAPSHOT et stockage dans Oracle
- **Utils_wks_pub.py** : Utilitaires pour la lecture du SNAPSHOT et tranformation pour Oracle
- **sql_admin.py** : Utilitaires de création/traitement de tables oracle

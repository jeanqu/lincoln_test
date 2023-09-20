# lincoln_test

## 1 Python et Data Engineering

### 1.3 Data pipeline

Pour répondre à ce problème nous allons découper notre réponse en 3 étapes : 
- Extraction des données sources. On voit rapidement qu'il y aura des problèmes de qualité de données à traiter (pubmed de 2 sources de données différentes, le champ date de clinial_trials).
- La jointure des données, via implémentation de la notion métier "drug mentionné dans.."
- l'écriture de l'unique fichier JSON de sortie.

Je vais me baser sur les notions d'Airflow pour orchestrer ces étapes. 


Une première solution serait de réaliser tous les traitements avec un seul opérateur. 
Celui-ci appellerait d'abord des connecteurs (hooks) pour aller lire les fichiers, adapter le format (caster les dates..) et les retournerait à l'opérateur dans un format python commun (panda, json, dictionnaire..). 
L'opérateur se chargerait ensuite de faire la jointure, puis un hook se chargerait de l'écriture.

Une seconde solution serait de découper ces étapes en plusieurs opérateurs. Les premiers auraient pour rôle d'extraire les données brutes, les cleaners, et les écrire dans de nouveaux fichiers, structurés (en format parquet par exemple). 
Puis un second opérateur ferait la jointure et écrirait le fichier final. 
Cette seconde solution est plus couteuse en stockage, car on ré écrit les fichiers d'inputs légèrement modifiés lors d'un étape intermédiaire. Mais cette étape intermédiaire peut justement être jugée utile, ces fichiers pouvant être ré utilisés pour d'autres besoins. Nous allons considérer que ces fichiers intermédiaires seront notre datawarehouse, alors que les fichiers d'inputs le datalake.
De plus la séparation en plusieurs opérateurs dans notre orchestrateur a de nombreux avantages : meilleure gestion des dates (ex : ne cleaner que les nouveaux articles n'ils arrivent dans l'ordre chronologique), gestion des erreurs plus simples (on ne debug et relance que l'opérateur à problème ), lecture du code plus simple.

Nous allons donc partir sur cette seconde solution. Je ne vais pas installer Airflow ou un autre orchestrateur, mais via le nommage de mes fichiers on pourra comprendre ou se retrouverait chaque fonction dans un tel projet.

Nous allons donc crée 3 datasets, dans le dossier 'data' : raw (l'input), cleaned (l'étape intermédiaire) et aggregated (l'output). On note que cela pourrait correspondre à datalake / datawarehous layer 1, datawarehouse layer 2.

Pour le format du graph nous allons privilégier la ré utilisation du fichier sortant. 
Pour cela nous allons le stocker comme si nous stockions une table sql, avec une ligne par publication de d'article. 
Un médicament aura donc plusieurs lignes. 
Cela gardera un format simple, ré utilisable. Une autre solution aurait été de fournir un json plus complexe, avec une clé unique de médicament, contenant des sous dossiers etc... 
Mais cette seconde utilisation complique grandement la récupération et le requétage de ces informations. 

Pour lancer le projet, depuis un nouveau venv : 
```
cd part_1/code
pip install -r requirements.txt
python -m drug_references_dag.py
```
Vous pourrez alors trouver le résultat dans data/aggregated/mentioned_drugs.json
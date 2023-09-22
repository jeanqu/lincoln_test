# lincoln_test

## 1 Python et Data Engineering

### 1.3 Data pipeline

Pour répondre à ce problème nous allons découper notre réponse en 3 étapes : 
- Extraction des données sources. On voit rapidement qu'il y aura des problèmes de qualité de données à traiter (pubmed de 2 sources de données différentes, le champ date de clinial_trials..).
- La jointure des données, via implémentation de la notion métier "drug mentionné dans.."
- l'écriture de l'unique fichier JSON de sortie.

Je vais me baser sur les notions d'Airflow pour orchestrer ces étapes. 

Une première solution serait de réaliser tous les traitements avec un seul opérateur. 
Celui-ci appellerait d'abord des connecteurs (hooks) pour aller lire les fichiers, adapter le format (caster les dates..) et les retournerait à l'opérateur dans un format python commun (panda, json, dictionnaire..). 
L'opérateur se chargerait ensuite de faire la jointure, puis un hook se chargerait de l'écriture.

Une seconde solution serait de découper ces étapes en plusieurs opérateurs. Les premiers auraient pour rôle d'extraire les données brutes, les cleaners, et les écrire dans de nouveaux fichiers, structurés (en format parquet par exemple). 
Puis un second opérateur ferait la jointure et écrirait le fichier final. 
Cette seconde solution est plus couteuse en stockage, car on ré écrit les fichiers d'inputs légèrement modifiés lors d'une étape intermédiaire. Mais cette étape intermédiaire peut justement être jugée utile, ces fichiers pouvant être ré utilisés pour d'autres besoins. On pourrait considérer que ces fichiers intermédiaires seront notre datawarehouse, alors que les fichiers d'inputs le datalake.
De plus la séparation en plusieurs opérateurs dans notre orchestrateur a de nombreux avantages : meilleure gestion des dates (ex : ne cleaner que les nouveaux articles s'ils arrivent dans l'ordre chronologique), gestion des erreurs plus simples (on ne debug et relance que l'opérateur à problème), lecture du code plus simple.

Nous allons donc partir sur cette seconde solution. Je ne vais pas installer Airflow ou un autre orchestrateur, mais via le nommage de mes fichiers on pourra comprendre ou se retrouverait chaque classe / fonction dans un tel projet.

Nous allons donc créer 3 datasets, dans le dossier 'data' : raw (l'input), cleaned (l'étape intermédiaire) et aggregated (l'output). On note que cela pourrait correspondre à datalake / datawarehous layer 1, datawarehouse layer 2.

Pour le format du graphe nous allons privilégier la ré utilisation du fichier sortant. 
Pour cela nous allons le stocker comme si nous stockions une table sql, avec une ligne par publication de d'article. 
Un médicament aura donc plusieurs lignes. 
Cela gardera un format simple, ré utilisable. 
Une autre solution aurait été de fournir un json plus complexe, avec une clé unique de médicament, contenant des sous dossiers etc... 
Mais cette seconde utilisation complique grandement la récupération et le requétage de ces informations. 

Pour lancer le projet, depuis un nouveau venv : 
```
cd part_1/src
pip install -r requirements.txt
python -m drug_references_dag.py
```
Vous pourrez alors trouver le résultat dans data/aggregated/mentioned_drugs.json

Pour lancer les tests, depuis le même dossier : 
```
pip install -r tests/requirements_test.txt
python -m pytest
```

On note que l'on pourrait ajouter des tests unitaires. 
Dans l'idéal on pourrait aussi ajouter des tests d'intégration.

#### Pour déployer le projet.

Nos scripts sont donc à déployer via un orchestrateur de DAG, type Airflow. 
Pour cela, l'idéal serait de brancher notre projet à des outils de CI / CD, type travis / concourse.
La CI se déclencherait à chaque commit, peut importe la branche. Elle lancerait les tests, et builderait l'image docker du projet. En cas d'échec d'une de ces 2 étapes, github bloquerait le merge dans la branche main.
Lors d'un merge dans main, on push l'image dans un repository d'image (ex : artifactory), avec un nouveau tag. Cela déclenchera à sont tour la CD, qui déploiera cette image dans kubernetes. 

Le déploiement kub et le build dépendront du type d'orchestrateur choisi, et ci celui ci est ditribué (airflow avec son schedulers et plusieurs workers), ou est une instance simple (un simple cron ou Airflow standalone). Dans les 2 cas on peut choisir : 
- soit de copier notre code (sans les tests) directement dans les images déployées, via le Dockerfile de ces images. 
- soit d'écrire notre code dans un ou plusieurs volumes séparés, qui seront lu par l'orchestrateur dans un second temps.


### 1.4 Traitement ad-hoc

Grace au format choisi lors de l'exercice précédent on va pouvoir simplement récupérer le fichier créé et répondre à cette question grace à une simple requête SQL.

En cas d'égalités entre 2 journaux, un seul apparaitra dans la solution.

Le script est dans le fichier `part_1/src/ad_hoc_get_best_journal_referer.py`

### 1.6 Pour aller plus loin

Pour lire des fichiers de plusieurs To la logique du DAG reste la même, mais il faudra adapter les outils utilisés..

Pour commencer les fichiers seraient à chercher non plus en local mais probablement dans le cloud, via s3 ou gcs. 
La bonne pratique serait d'avoir beaucoup fichiers, quelques fichiers de plusieurs To serait étrange. 
Le problème avec un seul fichier de plusieurs To est que l'outils choisi pour le lire ne pourra pas profiter de la parallélisation des workers, on devrait alors choisir un seul (très gros) worker, et attendre qu'il finisse tout seul de traiter tout le fichier, ce qui va créer un bottleneck. 
Avec plusieurs fichiers en inputs on pourra lancer plusieurs workers en parallèle qui se répartiront la tache, ce qui sera bien plus rapide et plus 'fault - tolerant'.

On pourrait ensuite les importer directement dans un moteur SQL analytics, comme Bigquery, dans leur format brute. 
On peut décider de faire l'étape en SQL après l'insertion dans Bigquery, ce qui serait plus lisible. 
Ou alors on peut copier les données des formats texte à SQL via Spark ou Dataflow, en y ajoutant une étape de cleaning de la donnée. 

Petite parenthèse. La meilleure solution pour régler ces soucis de qualité de donnée serait d'aller directement voir le produceurs de ces données, et trouver une solution pour que cela n'arrive plus. Utiliser un schéma entre le produceur et nous serait une bonne solution, en se partageant ces schémas via un repo commun (avro ou protobuf par exemple).

Une fois les données dans notre base d'analytics, la jointure, puis le re formatage se ferait via une requete SQL, tout simplement. 

Les moteurs donnent aussi des formats d'exports, si on veut toujours comme output un format json. La contrainte 'un seul fichier' ne pourrait plus être respectée, et on pousserait les données dans le cloud.

Via Airflow tous ces opérateurs sont déjà disponibles (si on utilise les technologies citées).
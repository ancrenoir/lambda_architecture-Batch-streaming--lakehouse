# WABA Financial Data Platform

Plateforme Data Lakehouse multi-pays réalisée pour le challenge Data Engineer
d'Artefact.

Le périmètre livré couvre les Levels 1 à 3 : génération de données, pipeline
batch, architecture Médaillon, orchestration Airflow et traitement streaming.
Le Level 4 (Kubernetes, Superset, gouvernance et observabilité) n'est pas
implémenté.

## Architecture

```text
Batch
Streamlit -> CSV validés avec Avro -> MinIO raw-landing
          -> Spark -> Iceberg Bronze / Silver / Gold -> Trino
                         ^
                         |
                      Airflow

Streaming
MinIO raw-landing -> NiFi -> Kafka Raw
                  -> Spark Job 1 -> Kafka Silver + Iceberg Silver
                  -> Spark Job 2 -> topics Gold (fraude, AML, liquidité)

Trino interroge les tables Iceberg et les topics Kafka.
```

### Partitionnement Iceberg

- **Exigence conservée :** les colonnes `country_code`, `entity_type` et la
  date métier restent dans les données. Les analyses par pays, entité et jour
  sont donc toujours possibles.
- **Choix physique :** dans les tables Gold, les colonnes de date déclarées
  comme partitions utilisent la transformation Iceberg `months(...)`. Deux
  lignes datées du 1er et du 2 octobre gardent leurs dates distinctes, mais
  leurs fichiers appartiennent à la même partition physique d'octobre.
- **Pourquoi :** une partition physique par jour aurait produit beaucoup de
  petites partitions, de petits fichiers et de métadonnées à gérer. La
  granularité mensuelle réduit ce coût sans supprimer la date quotidienne.
- **Limite :** une requête portant sur un seul jour peut lire davantage de
  fichiers qu'avec une partition quotidienne. Les couches Raw, Bronze et
  Silver restent actuellement partitionnées physiquement par pays et date.
  `entity_type` est une dimension métier, pas une partition physique.

Les huit contrats de données sont disponibles dans
[`schemas/avro`](schemas/avro). Le modèle relationnel est décrit dans
[`modelisation_relationnelle/waba_relational_model.dbml`](modelisation_relationnelle/waba_relational_model.dbml).

Le schéma de `products` n'étant pas détaillé dans l'annexe, il a été défini
dans le projet. La colonne `accounts.product_id` relie un compte au produit
commercial souscrit. Ce choix permet de contrôler la cohérence des comptes et
d'enrichir les tables Silver.

## Prérequis

- Git ;
- Docker avec Docker Compose v2 ;
- un terminal Bash ou WSL pour exécuter les commandes ci-dessous ;
- environ 12 Go de mémoire disponibles pour l'ensemble de la stack.

## 1. Récupérer et configurer le projet

```bash
git clone <URL_DU_DEPOT_GIT>
cd Challenge
cp .env.example .env
```

Ouvrir `.env`, puis remplacer toutes les valeurs commençant par
`replacer_...` ou `ajouter_...`. Le fichier `.env` est local et ne doit jamais
être poussé sur Git.

Générer les trois clés Airflow suivantes :

```bash
# Valeur de AIRFLOW_FERNET_KEY
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"

# Valeur de AIRFLOW_API_SECRET_KEY
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Valeur de AIRFLOW_JWT_SECRET : relancer la commande pour obtenir une autre clé
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Copier chaque résultat dans la variable indiquée. Conserver
`AIRFLOW_JWT_ALGORITHM=HS512`.

Vérifier ensuite que Compose lit correctement le fichier :

```bash
docker compose config --quiet
```

L'absence de message signifie que la configuration est valide.

## 2. Démarrer la stack

Depuis la racine du dépôt :

```bash
docker compose up -d --build
docker compose ps -a
```

Le premier démarrage télécharge ou construit les images et peut prendre
plusieurs minutes. Les services principaux doivent être `running` ou
`healthy`. Les services d'initialisation peuvent être `exited (0)` : cela
signifie qu'ils ont terminé correctement leur travail.

| Service | Adresse locale | Identifiants |
|---|---|---|
| Streamlit | <http://localhost:8501> | Aucun |
| MinIO | <http://localhost:9001> | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Airflow | <http://localhost:8081> | `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` |
| Spark Master | <http://localhost:8082> | Aucun |
| Trino | <http://localhost:8080> | CLI |
| NiFi | <https://localhost:8443/nifi> | `NIFI_ADMIN_USERNAME` / `NIFI_ADMIN_PASSWORD` |

NiFi utilise un certificat autosigné pour la démonstration locale. Lors de la
première ouverture, choisir les options avancées du navigateur puis continuer
vers `localhost`.

## 3. Générer les données dans Streamlit

Ouvrir <http://localhost:8501>.

Générer d'abord les référentiels dans cet ordre :

```text
customers + products + branches -> accounts
```

`accounts` dépend des trois autres référentiels. Cet ordre garantit que ses
clés étrangères existent.

Générer ensuite les transactions souhaitées :

- transactions bancaires ;
- opérations d'assurance ;
- paiements Mobile Money ;
- remboursements de prêts.

Deux modes sont proposés :

- `One-time` : génération immédiate d'un fichier ;
- `Continue` : génération répétée toutes les 10 à 60 secondes pour simuler un
  flux.

Les paramètres permettent de choisir les pays, la ligne métier, la période et
le volume. Les lignes sont contrôlées avec le contrat Avro correspondant, puis
les CSV sont envoyés dans le bucket MinIO `raw-landing`.

## 4. Exécuter le pipeline batch

Ouvrir Airflow sur <http://localhost:8081>, puis déclencher
`dag_ingest_raw`.

Le paramètre `country_code` accepte :

- `CI`, `SN`, `ML`, `BF`, `GN`, `TG`, `BJ` ou `GH` pour un pays ;
- `ALL_COUNTRY` pour tous les pays.

Le pipeline s'enchaîne ainsi :

```text
dag_ingest_raw -> dag_bronze_to_silver -> dag_silver_to_gold
```

Le graphe Airflow permet de suivre chaque tâche, ses retries et ses logs. Le
DAG `dag_regulatory_report` est planifié chaque jour à 00 h 30 UTC et peut
aussi être déclenché manuellement.

Vérifier ensuite l'accès aux tables Iceberg avec Trino :

```bash
docker compose exec trino trino \
  --server http://localhost:8080 \
  --user waba \
  --execute "SHOW SCHEMAS FROM data"

docker compose exec trino trino \
  --server http://localhost:8080 \
  --user waba \
  --execute "SELECT * FROM data.gold.daily_transaction_volume LIMIT 10"
```

La première commande liste les schémas Iceberg connus du catalogue `data`.
La seconde lit dix lignes du KPI bancaire journalier.

## 5. Activer le flux NiFi

Le flux reproductible est exporté dans
[`nifi/flowfile_group_process/raw_landing_to_raw_kafka.json`](nifi/flowfile_group_process/raw_landing_to_raw_kafka.json).

Dans NiFi :

1. ouvrir le canvas principal ;
2. importer le fichier JSON comme Process Group ;
3. ouvrir le groupe `raw_landing_to_raw_kafka` ;
4. vérifier le Parameter Context `waba_streaming` ;
5. activer les Controller Services ;
6. démarrer le Process Group.

NiFi surveille `raw-landing`, récupère chaque nouveau CSV de transaction,
transforme chaque ligne en événement JSON, ajoute `source_file` et
`ingestion_timestamp`, puis publie l'événement dans le topic Kafka Raw adapté.

`ListS3` conserve un état. Pour rejouer une démonstration, déposer un fichier
portant un nouveau nom plutôt que remettre exactement le même objet.

## 6. Exécuter les jobs streaming

Attendre que `airflow-scheduler`, `spark-master` et `spark-worker` soient
actifs.

Le Job 1 contrôle les événements Raw, place les rejets dans la DLQ et écrit les
événements valides dans Kafka Silver et dans les tables Iceberg Silver :

```bash
docker compose exec -T airflow-scheduler spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=airflow-scheduler \
  /opt/airflow/spark/jobs/streaming_job_raw_to_silver.py \
  --table bank_transactions
```

Le Job 2 lit Kafka Silver et publie les alertes Gold de fraude, AML et
liquidité :

```bash
docker compose exec -T airflow-scheduler spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=airflow-scheduler \
  /opt/airflow/spark/jobs/streaming_job_silver_to_gold.py \
  --table bank_transactions
```

Ces commandes restent actives car il s'agit de traitements streaming. Ouvrir
un terminal séparé pour chaque job. Les valeurs `insurance_operations` et
`mobile_money_payments` permettent de lancer les autres domaines.

Exemples de contrôle :

```bash
# Événements bancaires valides
docker compose exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic silver-bank-transactions \
  --from-beginning \
  --timeout-ms 10000

# Alertes de fraude
docker compose exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic gold-fraud-alerts \
  --from-beginning \
  --timeout-ms 10000
```

## 7. Exécuter les requêtes Lambda Trino

La requête détaillée réunit l'historique Gold antérieur au jour courant et les
événements Kafka Silver du jour courant :

```bash
docker compose exec -T trino trino \
  --server http://localhost:8080 \
  --user waba \
  < trino/query/lambda_bank_transaction/lambda_bank_transactions_detail.sql
```

La seconde requête présente une vue agrégée par jour et pays :

```bash
docker compose exec -T trino trino \
  --server http://localhost:8080 \
  --user waba \
  < trino/query/lambda_bank_transaction/lambda_transactions_global.sql
```

La séparation `historique < jour courant` / `streaming = jour courant` évite
de compter deux fois une transaction présente dans les deux chemins.

## 8. Tester les générateurs

```bash
docker compose exec -T streamlit_dev \
  python -m unittest tests/test_data_generation.py -v
```

Cette commande :

1. entre dans le conteneur Streamlit déjà démarré ;
2. génère un petit jeu des huit tables dans un dossier temporaire ;
3. vérifie les volumes et les principales relations entre clés étrangères ;
4. supprime automatiquement les fichiers temporaires à la fin.

Elle ne dépose rien dans MinIO et ne modifie donc pas la démonstration.
`-T` désactive le terminal interactif, `-m unittest` lance le module de tests
Python et `-v` affiche le détail de chaque test.

Lister les onze topics Kafka attendus :

```bash
docker compose exec broker kafka-topics \
  --bootstrap-server broker:29092 \
  --list
```

Ils comprennent quatre topics Raw, trois Silver, trois Gold et une DLQ.

Lire les événements rejetés :

```bash
docker compose exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic dlq-financial-events \
  --from-beginning \
  --timeout-ms 10000
```

## Limites connues

- Le Level 4 n'est pas implémenté.
- Les taux XOF/GHS vers EUR sont fixes afin de rendre les résultats
  reproductibles. En production, ils viendraient d'une source de taux de
  change versionnée.
- L'alerte de liquidité est une règle de démonstration : elle estime le solde
  après les flux récents et alerte lorsque le ratio de couverture passe sous
  20 %. Elle ne reproduit pas une formule réglementaire BCEAO complète.
- Chaque service ne possède qu'une seule instance et les échanges entre
  services utilisent le réseau Docker local sans TLS. Cette configuration est
  adaptée à une démonstration locale, mais n'apporte ni haute disponibilité ni
  chiffrement interservices.
- Les CSV traités avec succès restent dans `raw-landing` : leur déplacement
  automatique vers `archive` n'est pas implémenté. Les lignes rejetées sont en
  revanche conservées avec leur motif.
- Le catalogue de produits est une hypothèse de modélisation, car son schéma
  n'est pas fourni dans l'annexe du challenge.

## Arrêter la stack

```bash
docker compose down --remove-orphans
```

Les volumes Docker sont conservés. Pour une nouvelle démonstration, utiliser
de nouveaux noms de fichiers afin que NiFi les détecte comme de nouveaux
objets.

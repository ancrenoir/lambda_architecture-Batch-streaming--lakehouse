import csv
import json
import random
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path

from fastavro.schema import parse_schema
from fastavro.validation import validate


# --- CONFIG WABA GROUP ---
COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
UEMOA = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]
CURRENCY_MAP = {country: "XOF" for country in UEMOA} | {"GH": "GHS"}

ENTITY_TYPES = ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"]

#Proportion de generation par entite
ENTITY_PROBS = {
    "BANK": 0.55,
    "INSURANCE": 0.20,
    "MOBILE_MONEY": 0.18,
    "MICROFINANCE": 0.07,
}

#Type Entites ex bank, present dans quel pays
ENTITY_COUNTRIES = {
    "BANK": COUNTRIES,
    "INSURANCE": COUNTRIES,
    "MOBILE_MONEY": ["CI", "SN", "BF", "GH"],
    "MICROFINANCE": ["ML", "GN", "BF"],
}

# Hypothese documentee pour la demonstration de fraude du Level 3 :
# une faible part des paiements est initiee depuis un pays different du pays
# habituel du client emetteur.
MOBILE_UNUSUAL_COUNTRY_PROBABILITY = 0.02

SEGMENTS = ["RETAIL", "SME", "CORPORATE", "PREMIUM"]
SEGMENT_PROBS = [0.65, 0.20, 0.10, 0.05]
KYC_LEVELS = ["BASIC", "STANDARD", "ENHANCED"]
KYC_PROBS = [0.30, 0.50, 0.20]

TXN_STATUSES = ["SUCCESS", "FAILED", "REVERSED"]
TXN_PROBS = [0.92, 0.05, 0.03]

# Cibles réglementaires demandées par le challenge.
# Les prêts DEFAULT doivent représenter entre 3 % et 8 % des encours simulés.
NPL_TARGETS = {
    "CI": 0.04,
    "SN": 0.05,
    "ML": 0.06,
    "BF": 0.07,
    "GN": 0.05,
    "TG": 0.04,
    "BJ": 0.06,
    "GH": 0.075,
}

# Ratio cible : sinistres payés / primes acquises, par pays.
# Les valeurs sont volontairement à l'intérieur de la plage attendue 50 %–85 %
# afin de conserver une marge face à l'aléa de génération.
LOSS_RATIO_TARGETS = {
    "CI": 0.60,
    "SN": 0.55,
    "ML": 0.65,
    "BF": 0.72,
    "GN": 0.58,
    "TG": 0.62,
    "BJ": 0.68,
    "GH": 0.78,
}

REGIONS = {
    "CI": ["Abidjan", "Bouaké", "Yamoussoukro", "San Pedro", "Korhogo"],
    "SN": ["Dakar", "Thiès", "Ziguinchor", "Saint-Louis", "Kaolack"],
    "ML": ["Bamako", "Sikasso", "Ségou", "Mopti", "Tombouctou"],
    "BF": ["Ouagadougou", "Bobo-Dioulasso", "Koudougou", "Banfora"],
    "GN": ["Conakry", "Nzérékoré", "Kindia", "Kankan"],
    "TG": ["Lomé", "Sokodé", "Kara", "Atakpamé"],
    "BJ": ["Cotonou", "Porto-Novo", "Parakou", "Abomey-Calavi"],
    "GH": ["Accra", "Kumasi", "Tamale", "Cape Coast", "Sunyani"],
}

CUSTOMERS_COLUMNS = [
    "customer_id",
    "country_code",
    "entity_type",
    "segment",
    "kyc_level",
    "onboarding_date",
    "region",
    "is_active",
]

PRODUCTS_COLUMNS = [
    "product_id",
    "product_name",
    "country_code",
    "entity_type",
    "account_type",
    "target_segment",
    "product_line",
    "loan_type",
    "is_active",
]

BRANCHES_COLUMNS = [
    "branch_id",
    "country_code",
    "entity_type",
    "city",
    "region",
    "branch_type",
    "is_active",
]

ACCOUNTS_COLUMNS = [
    "account_id",
    "customer_id",
    "product_id",
    "country_code",
    "entity_type",
    "account_type",
    "currency",
    "balance",
    "credit_limit",
    "opened_date",
    "status",
]

BANK_TRANSACTIONS_COLUMNS = [
    "transaction_id",
    "timestamp",
    "account_id",
    "beneficiary_account",
    "branch_id",
    "country_code",
    "transaction_type",
    "amount",
    "currency",
    "channel",
    "transaction_status",
    "fee_amount",
    "entity_type",
]

INSURANCE_OPERATIONS_COLUMNS = [
    "operation_id",
    "timestamp",
    "customer_id",
    "account_id",
    "country_code",
    "operation_type",
    "product_line",
    "amount",
    "currency",
    "claim_status",
    "processing_days",
    "entity_type",
]

MOBILE_MONEY_COLUMNS = [
    "payment_id",
    "timestamp",
    "sender_id",
    "receiver_id",
    "sender_country",
    "receiver_country",
    "country_code",
    "amount",
    "currency",
    "operator",
    "status",
    "fee_amount",
    "entity_type",
]

LOAN_REPAYMENTS_COLUMNS = [
    "repayment_id",
    "timestamp",
    "loan_account_id",
    "customer_id",
    "country_code",
    "amount_due",
    "amount_paid",
    "currency",
    "due_date",
    "payment_date",
    "days_overdue",
    "loan_type",
    "repayment_status",
    "entity_type",
]

''' 
    Un produit est ratache à un type de compte souscrit par un individue ou une personne moral.
    Le dict des products represente pour chaque entite ex: Bank, ses types de produit
    selon les choix de produit effectue arbitrairement
    un tuple represente un produit ratache à ses corespondante valide (account_type, segment,
    product line (est specifique au segemt qui ont des sous categorie comme insurrance vie)), loan_type le meme raisonnement pour )
    Ainsi les None present dans les tuple sont pour les produit dont l'attribue ne s'applique pas
    typle (product_name, account_type, target_segment, product_line, loan_type)
''' 
PRODUCT_TEMPLATES = {
    "BANK": [
        ("Prêt Conso Express", "LOAN", "RETAIL", None, "CONSUMER"),
        ("Compte Essentiel", "CURRENT", "RETAIL", None, None),
        ("Épargne Avenir", "SAVINGS", "RETAIL", None, None),
        ("Compte Étudiant", "CURRENT", "RETAIL", None, None),
        ("Financement PME", "LOAN", "SME", None, "SME"),
        ("Compte Medium", "CURRENT", "SME", None, None),
        ("Compte Premium", "CURRENT", "PREMIUM", None, None),
        ("Crédit Immobilier", "LOAN", "RETAIL", None, "MORTGAGE"),
        ("Crédit Agricole", "LOAN", "SME", None, "AGRICULTURAL"),
    ],
    "INSURANCE": [
        ("Assurance Vie Avenir", "INSURANCE_POLICY", "RETAIL", "VIE", None),
        ("Auto Essentiel", "INSURANCE_POLICY", "RETAIL", "IARD_AUTO", None),
        ("Auto Confort", "INSURANCE_POLICY", "PREMIUM", "IARD_AUTO", None),
        ("Habitation Sérénité", "INSURANCE_POLICY", "RETAIL", "IARD_HABITATION", None),
        ("Santé Famille", "INSURANCE_POLICY", "RETAIL", "IARD_SANTE", None),
        ("Prévoyance Entreprise", "INSURANCE_POLICY", "CORPORATE", "PREVOYANCE", None),
    ],
    "MOBILE_MONEY": [
        ("WABA Pay Standard", "MOBILE_WALLET", "RETAIL", None, None),
        ("WABA Pay Business", "MOBILE_WALLET", "SME", None, None),
        ("WABA Pay Premium", "MOBILE_WALLET", "PREMIUM", None, None),
    ],
    "MICROFINANCE": [
        ("Microcrédit Solidaire", "LOAN", "RETAIL", None, "MICROCREDIT"),
        ("Compte Microfinance", "SAVINGS", "RETAIL", None, None),
        ("Épargne Tontine", "SAVINGS", "RETAIL", None, None),
        ("Crédit Agricole", "LOAN", "SME", None, "AGRICULTURAL"),
    ],
}

#Construction dynamique du repertoire independement du systeme, en placant à la racine du dossier
''' Cas d'usage schema avro pour le contrat de schema pour la validation des donnee generer
    avant ecriture dans les csv brute aucune modificationde la qualite des donnees genere à cette etape.'''

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIRECTORY = PROJECT_ROOT / "schemas" / "avro"


def load_schema(schema_name):
    schema_path = SCHEMA_DIRECTORY / f"{schema_name}.avsc"
    with open(schema_path, "r", encoding="utf-8") as schema_file:
        return parse_schema(json.load(schema_file))


CUSTOMERS_SCHEMA = load_schema("customers")
PRODUCTS_SCHEMA = load_schema("products")
BRANCHES_SCHEMA = load_schema("branches")
ACCOUNTS_SCHEMA = load_schema("accounts")
BANK_TRANSACTIONS_SCHEMA = load_schema("bank_transactions")
INSURANCE_OPERATIONS_SCHEMA = load_schema("insurance_operations")
MOBILE_MONEY_SCHEMA = load_schema("mobile_money_payments")
LOAN_REPAYMENTS_SCHEMA = load_schema("loan_repayments")



def random_timestamp(start, end, random_generator=random):
    delta = end - start
    return start + timedelta(
        seconds=random_generator.randint(0, int(delta.total_seconds()))
    )


def random_uuid(random_generator):
    return str(uuid.UUID(int=random_generator.getrandbits(128), version=4))


def choose_record_except(records, id_field, excluded_value, random_generator):
    if not records:
        raise ValueError("Aucun enregistrement compatible n'est disponible.")

    selected_index = random_generator.randrange(len(records))
    selected_record = records[selected_index]
    if selected_record[id_field] != excluded_value:
        return selected_record

    if len(records) == 1:
        raise ValueError("Aucun autre enregistrement compatible n'est disponible.")

    return records[(selected_index + 1) % len(records)]

#Permet verifier et recuperer par entite  les pays ou elle existe
def eligible_country_entity_pairs(countries, entity_types):
    return [
        (country, entity)
        for entity in entity_types
        for country in countries
        if country in ENTITY_COUNTRIES[entity]
    ]


def validate_generation_parameters(countries, entity_types, n_rows):
    if not countries:
        raise ValueError("Sélectionnez au moins un pays.")

    if not entity_types:
        raise ValueError("Sélectionnez au moins une ligne métier.")

    unknown_countries = set(countries) - set(COUNTRIES)
    if unknown_countries:
        raise ValueError(f"Pays non autorisés : {sorted(unknown_countries)}")

    unknown_entities = set(entity_types) - set(ENTITY_TYPES)
    if unknown_entities:
        raise ValueError(f"Lignes métier non autorisées : {sorted(unknown_entities)}")

    if n_rows <= 0:
        raise ValueError("Le nombre de lignes doit être supérieur à zéro.")

    if n_rows < len(countries):
        raise ValueError(
            "Le nombre de lignes doit être au moins égal au nombre de pays "
            "pour représenter chaque pays sélectionné."
        )

    for country in countries:
        if not any(country in ENTITY_COUNTRIES[entity] for entity in entity_types):
            raise ValueError(
                f"Aucune ligne métier sélectionnée n'est disponible pour {country}."
            )


def validate_period(start_date, end_date):
    if start_date is None or end_date is None:
        raise ValueError("Les dates de début et de fin sont obligatoires.")
    if start_date > end_date:
        raise ValueError("La date de début doit précéder la date de fin.")


def validate_transaction_parameters(country, n_rows):
    if country not in COUNTRIES:
        raise ValueError(f"Pays non autorisé : {country}")
    if n_rows <= 0:
        raise ValueError("Le nombre de lignes doit être supérieur à zéro.")


def generate_customers(
    output_path,
    countries,
    entity_types,
    n_rows=500_000,
    seed=42,
    reference_date=None,
):
    validate_generation_parameters(countries, entity_types, n_rows)

    reference_date = reference_date or date.today()
    if reference_date < date(2010, 1, 1):
        raise ValueError("La date de référence doit être postérieure au 01/01/2010.")

    random_generator = random.Random(seed)
    country_entity_pairs = eligible_country_entity_pairs(countries, entity_types)
    output_path = Path(output_path)

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(CUSTOMERS_COLUMNS)

        for customer_number in range(1, n_rows + 1):
            if customer_number <= len(country_entity_pairs):
                country, entity_type = country_entity_pairs[customer_number - 1]
            else:
                country = countries[(customer_number - 1) % len(countries)]
                compatible_entities = [
                    entity
                    for entity in entity_types
                    if country in ENTITY_COUNTRIES[entity]
                ]
                entity_weights = [
                    ENTITY_PROBS[entity] for entity in compatible_entities
                ]
                entity_type = random_generator.choices(
                    compatible_entities,
                    weights=entity_weights,
                    k=1,
                )[0]

            customer = {
                "customer_id": f"WABA-{country}-C-{customer_number:06d}",
                "country_code": country,
                "entity_type": entity_type,
                "segment": random_generator.choices(
                    SEGMENTS, weights=SEGMENT_PROBS, k=1
                )[0],
                "kyc_level": random_generator.choices(
                    KYC_LEVELS, weights=KYC_PROBS, k=1
                )[0],
                "onboarding_date": random_timestamp(
                    datetime(2010, 1, 1),
                    datetime.combine(reference_date, time.min),
                    random_generator,
                ).date(),
                "region": random_generator.choice(REGIONS[country]),
                "is_active": (
                    True
                    if customer_number <= len(country_entity_pairs)
                    else random_generator.random() > 0.08
                ),
            }

            validate(customer, CUSTOMERS_SCHEMA, raise_errors=True)
            writer.writerow(
                [
                    customer["customer_id"],
                    customer["country_code"],
                    customer["entity_type"],
                    customer["segment"],
                    customer["kyc_level"],
                    customer["onboarding_date"].isoformat(),
                    customer["region"],
                    customer["is_active"],
                ]
            )

    return output_path


def generate_products(
    output_path,
    countries,
    entity_types,
    n_rows=50,
    seed=42,
):
    validate_generation_parameters(countries, entity_types, n_rows)
    random_generator = random.Random(seed)
    country_entity_pairs = eligible_country_entity_pairs(countries, entity_types)
    output_path = Path(output_path)

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(PRODUCTS_COLUMNS)

        for product_number in range(1, n_rows + 1):
            country, entity_type = country_entity_pairs[
                (product_number - 1) % len(country_entity_pairs)
            ]
            template_number = (product_number - 1) // len(country_entity_pairs)
            template = PRODUCT_TEMPLATES[entity_type][
                template_number % len(PRODUCT_TEMPLATES[entity_type])
            ]
            product_name, account_type, segment, product_line, loan_type = template

            product = {
                "product_id": f"WABA-{country}-P-{product_number:04d}",
                "product_name": product_name,
                "country_code": country,
                "entity_type": entity_type,
                "account_type": account_type,
                "target_segment": segment,
                "product_line": product_line,
                "loan_type": loan_type,
                "is_active": (
                    True
                    if product_number <= len(country_entity_pairs)
                    else random_generator.random() > 0.05
                ),
            }

            validate(product, PRODUCTS_SCHEMA, raise_errors=True)
            writer.writerow(
                [
                    product["product_id"],
                    product["product_name"],
                    product["country_code"],
                    product["entity_type"],
                    product["account_type"],
                    product["target_segment"],
                    product["product_line"] or "",
                    product["loan_type"] or "",
                    product["is_active"],
                ]
            )

    return output_path


def generate_branches(
    output_path,
    countries,
    entity_types,
    n_rows=200,
    seed=42,
):
    validate_generation_parameters(countries, entity_types, n_rows)
    random_generator = random.Random(seed)
    country_entity_pairs = eligible_country_entity_pairs(countries, entity_types)
    branch_types = ["FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT"]
    branch_type_probs = [0.45, 0.15, 0.25, 0.15]
    output_path = Path(output_path)

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(BRANCHES_COLUMNS)

        #permet de faire en boucle le tupe de la paire pays-entite pour ne pas avoir a genere des branch ratache à une seule paire
        for branch_number in range(1, n_rows + 1):
            country, entity_type = country_entity_pairs[
                (branch_number - 1) % len(country_entity_pairs)
            ]
            city = random_generator.choice(REGIONS[country])
            branch = {
                "branch_id": f"WABA-{country}-B-{branch_number:03d}",
                "country_code": country,
                "entity_type": entity_type,
                "city": city,
                "region": f"Région de {city}",
                "branch_type": random_generator.choices(
                    branch_types, weights=branch_type_probs, k=1
                )[0],
                "is_active": (
                    True
                    if branch_number <= len(country_entity_pairs)
                    else random_generator.random() > 0.05
                ),
            }

            validate(branch, BRANCHES_SCHEMA, raise_errors=True)
            writer.writerow([branch[column] for column in BRANCHES_COLUMNS])

    return output_path


def generate_accounts(
    output_path,
    customers,
    products,
    countries,
    entity_types,
    n_rows=800_000,
    seed=42,
    reference_date=None,
):
    validate_generation_parameters(countries, entity_types, n_rows)
    reference_date = reference_date or date.today()
    random_generator = random.Random(seed)
    output_path = Path(output_path)

    products_by_pair = {}
    products_by_pair_and_segment = {}
    for product in products:
        pair = (product["country_code"], product["entity_type"])
        if product["is_active"].lower() != "true":
            continue
        products_by_pair.setdefault(pair, []).append(product)
        segment_key = pair + (product["target_segment"],)
        products_by_pair_and_segment.setdefault(segment_key, []).append(product)

    eligible_customers = [
        customer
        for customer in customers
        if customer["country_code"] in countries
        and customer["entity_type"] in entity_types
        and (customer["country_code"], customer["entity_type"]) in products_by_pair
    ]
    if not eligible_customers:
        raise ValueError(
            "Aucun client ne possède de produit compatible. "
            "Générez d'abord customers.csv et products.csv avec les mêmes "
            "pays et métiers."
        )

    represented_countries = {
        customer["country_code"] for customer in eligible_customers
    }
    missing_countries = set(countries) - represented_countries
    if missing_countries:
        raise ValueError(
            "Aucun couple client-produit compatible pour les pays : "
            f"{sorted(missing_countries)}"
        )

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(ACCOUNTS_COLUMNS)

        for account_number in range(1, n_rows + 1):
            customer = eligible_customers[
                (account_number - 1) % len(eligible_customers)
            ]
            pair = (customer["country_code"], customer["entity_type"])
            segment_key = pair + (customer["segment"],)
            compatible_products = products_by_pair_and_segment.get(
                segment_key, products_by_pair[pair]
            )
            product = random_generator.choice(compatible_products)
            onboarding_date = date.fromisoformat(customer["onboarding_date"])
            opened_date = random_timestamp(
                datetime.combine(onboarding_date, time.min),
                datetime.combine(reference_date, time.min),
                random_generator,
            ).date()

            balance = round(max(0, random_generator.lognormvariate(11, 1.2)), 2)
            credit_limit = 0.0
            if product["account_type"] in {"CURRENT", "LOAN"}:
                credit_limit = round(random_generator.uniform(50_000, 5_000_000), 2)

            account = {
                "account_id": (
                    f"WABA-{customer['country_code']}-A-{account_number:07d}"
                ),
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "country_code": customer["country_code"],
                "entity_type": customer["entity_type"],
                "account_type": product["account_type"],
                "currency": CURRENCY_MAP[customer["country_code"]],
                "balance": balance,
                "credit_limit": credit_limit,
                "opened_date": opened_date,
                "status": (
                    "ACTIVE"
                    if account_number <= len(eligible_customers)
                    else random_generator.choices(
                        ["ACTIVE", "FROZEN", "CLOSED", "DORMANT"],
                        weights=[0.85, 0.04, 0.06, 0.05],
                        k=1,
                    )[0]
                ),
            }

            validate(account, ACCOUNTS_SCHEMA, raise_errors=True)
            writer.writerow(
                [
                    account["account_id"],
                    account["customer_id"],
                    account["product_id"],
                    account["country_code"],
                    account["entity_type"],
                    account["account_type"],
                    account["currency"],
                    account["balance"],
                    account["credit_limit"],
                    account["opened_date"].isoformat(),
                    account["status"],
                ]
            )

    return output_path


def generate_bank_transactions(
    output_path,
    accounts,
    branches,
    country,
    entity_types,
    start_date,
    end_date,
    n_rows=10_000,
    seed=42,
):
    validate_transaction_parameters(country, n_rows)
    validate_period(start_date, end_date)
    allowed_entities = [
        entity for entity in entity_types if entity in {"BANK", "MICROFINANCE"}
    ]
    if not allowed_entities:
        raise ValueError("Les transactions bancaires exigent BANK ou MICROFINANCE.")

    random_generator = random.Random(seed)
    active_accounts = []
    accounts_by_country = {}
    for account in accounts:
        if (
            account["entity_type"] in allowed_entities
            and account["account_type"] in {"CURRENT", "SAVINGS", "LOAN"}
            and account["status"] == "ACTIVE"
        ):
            active_accounts.append(account)
            accounts_by_country.setdefault(account["country_code"], []).append(
                account
            )

    branches_by_entity = {}
    for branch in branches:
        if (
            branch["country_code"] == country
            and branch["entity_type"] in allowed_entities
            and branch["is_active"].lower() == "true"
        ):
            branches_by_entity.setdefault(branch["entity_type"], []).append(branch)

    source_accounts = [
        account
        for account in accounts_by_country.get(country, [])
        if account["entity_type"] in branches_by_entity
    ]
    international_accounts = [
        account
        for account in active_accounts
        if account["country_code"] != country
    ]

    if not source_accounts or len(active_accounts) < 2 or not branches_by_entity:
        raise ValueError(
            "Référentiels insuffisants pour générer les transactions bancaires "
            f"de {country}."
        )

    txn_types = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE"]
    txn_probs = [0.35, 0.30, 0.15, 0.15, 0.05]
    channels = ["BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD"]
    channel_probs = [0.20, 0.15, 0.35, 0.20, 0.10]
    output_path = Path(output_path)

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(BANK_TRANSACTIONS_COLUMNS)

        for _ in range(n_rows):
            source_account = random_generator.choice(source_accounts)
            transaction_type = random_generator.choices(
                txn_types, weights=txn_probs, k=1
            )[0]
            if transaction_type == "INTERNATIONAL_WIRE":
                beneficiary_pool = international_accounts or active_accounts
            else:
                beneficiary_pool = accounts_by_country.get(country, [])
                if len(beneficiary_pool) < 2:
                    beneficiary_pool = active_accounts

            beneficiary = choose_record_except(
                beneficiary_pool,
                "account_id",
                source_account["account_id"],
                random_generator,
            )
            branch_candidates = branches_by_entity[source_account["entity_type"]]
            amount = round(max(500, random_generator.lognormvariate(12, 1.5)), 2)
            status = random_generator.choices(TXN_STATUSES, weights=TXN_PROBS, k=1)[0]
            transaction = {
                "transaction_id": random_uuid(random_generator),
                "timestamp": random_timestamp(
                    datetime.combine(start_date, time.min),
                    datetime.combine(end_date, time.max),
                    random_generator,
                ),
                "account_id": source_account["account_id"],
                "beneficiary_account": beneficiary["account_id"],
                "branch_id": random_generator.choice(branch_candidates)["branch_id"],
                "country_code": country,
                "transaction_type": transaction_type,
                "amount": amount,
                "currency": source_account["currency"],
                "channel": random_generator.choices(
                    channels, weights=channel_probs, k=1
                )[0],
                "transaction_status": status,
                "fee_amount": round(amount * 0.001, 2) if status == "SUCCESS" else 0.0,
                "entity_type": source_account["entity_type"],
            }

            validate(transaction, BANK_TRANSACTIONS_SCHEMA, raise_errors=True)
            writer.writerow(
                [
                    transaction["transaction_id"],
                    transaction["timestamp"].isoformat(),
                    transaction["account_id"],
                    transaction["beneficiary_account"],
                    transaction["branch_id"],
                    transaction["country_code"],
                    transaction["transaction_type"],
                    transaction["amount"],
                    transaction["currency"],
                    transaction["channel"],
                    transaction["transaction_status"],
                    transaction["fee_amount"],
                    transaction["entity_type"],
                ]
            )

    return output_path


def generate_insurance_operations(
    output_path,
    accounts,
    products,
    country,
    start_date,
    end_date,
    n_rows=5_000,
    seed=42,
):
    validate_transaction_parameters(country, n_rows)
    validate_period(start_date, end_date)
    random_generator = random.Random(seed)
    product_by_id = {product["product_id"]: product for product in products}
    insurance_accounts = [
        account
        for account in accounts
        if account["country_code"] == country
        and account["entity_type"] == "INSURANCE"
        and account["account_type"] == "INSURANCE_POLICY"
        and account["status"] == "ACTIVE"
        and account["product_id"] in product_by_id
        and product_by_id[account["product_id"]]["product_line"]
    ]
    if not insurance_accounts:
        raise ValueError(
            f"Aucune police d'assurance active n'est disponible pour {country}."
        )

    operation_types = [
        "PREMIUM_PAYMENT",
        "CLAIM_SUBMISSION",
        "CLAIM_PAYMENT",
        "POLICY_RENEWAL",
        "POLICY_CANCELLATION",
    ]
    premium_probability = 0.40
    claim_submission_probability = 0.15
    cancellation_probability = 0.08
    claim_payment_probability = (
        premium_probability * LOSS_RATIO_TARGETS[country]
    )
    renewal_probability = (
        1
        - premium_probability
        - claim_submission_probability
        - claim_payment_probability
        - cancellation_probability
    )
    operation_probs = [
        premium_probability,
        claim_submission_probability,
        claim_payment_probability,
        renewal_probability,
        cancellation_probability,
    ]
    output_path = Path(output_path)

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(INSURANCE_OPERATIONS_COLUMNS)

        for _ in range(n_rows):
            account = random_generator.choice(insurance_accounts)
            product = product_by_id[account["product_id"]]
            operation_type = random_generator.choices(
                operation_types, weights=operation_probs, k=1
            )[0]
            claim_status = None
            processing_days = None
            if operation_type == "CLAIM_SUBMISSION":
                claim_status = random_generator.choices(
                    ["PENDING", "APPROVED", "REJECTED", "PAID"],
                    weights=[0.45, 0.30, 0.20, 0.05],
                    k=1,
                )[0]
                processing_days = random_generator.randint(1, 45)
            elif operation_type == "CLAIM_PAYMENT":
                claim_status = "PAID"
                processing_days = random_generator.randint(2, 45)

            operation = {
                "operation_id": random_uuid(random_generator),
                "timestamp": random_timestamp(
                    datetime.combine(start_date, time.min),
                    datetime.combine(end_date, time.max),
                    random_generator,
                ),
                "customer_id": account["customer_id"],
                "account_id": account["account_id"],
                "country_code": country,
                "operation_type": operation_type,
                "product_line": product["product_line"],
                "amount": round(
                    max(1_000, random_generator.lognormvariate(11, 1.2)), 2
                ),
                "currency": account["currency"],
                "claim_status": claim_status,
                "processing_days": processing_days,
                "entity_type": "INSURANCE",
            }

            validate(operation, INSURANCE_OPERATIONS_SCHEMA, raise_errors=True)
            writer.writerow(
                [
                    operation["operation_id"],
                    operation["timestamp"].isoformat(),
                    operation["customer_id"],
                    operation["account_id"],
                    operation["country_code"],
                    operation["operation_type"],
                    operation["product_line"],
                    operation["amount"],
                    operation["currency"],
                    operation["claim_status"] or "",
                    ""
                    if operation["processing_days"] is None
                    else operation["processing_days"],
                    operation["entity_type"],
                ]
            )

    return output_path


def generate_mobile_money_payments(
    output_path,
    customers,
    country,
    start_date,
    end_date,
    n_rows=20_000,
    seed=42,
):
    validate_transaction_parameters(country, n_rows)
    validate_period(start_date, end_date)
    random_generator = random.Random(seed)
    mobile_customers = []
    customers_by_country = {}
    for customer in customers:
        if (
            customer["entity_type"] == "MOBILE_MONEY"
            and customer["is_active"].lower() == "true"
        ):
            mobile_customers.append(customer)
            customers_by_country.setdefault(customer["country_code"], []).append(
                customer
            )

    senders = customers_by_country.get(country, [])
    cross_border_receivers = [
        customer
        for customer in mobile_customers
        if customer["country_code"] != country
    ]
    unusual_sender_countries = [
        mobile_country
        for mobile_country in ENTITY_COUNTRIES["MOBILE_MONEY"]
        if mobile_country != country
    ]
    if not senders or len(mobile_customers) < 2:
        raise ValueError(
            "Référentiel clients insuffisant pour les paiements mobile money "
            f"de {country}."
        )

    operators = ["WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"]
    statuses = ["SUCCESS", "FAILED", "PENDING"]
    status_probs = [0.94, 0.04, 0.02]
    output_path = Path(output_path)

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(MOBILE_MONEY_COLUMNS)

        for _ in range(n_rows):
            sender = random_generator.choice(senders)
            if cross_border_receivers and random_generator.random() < 0.20:
                receiver_pool = cross_border_receivers
            elif len(senders) > 1:
                receiver_pool = senders
            else:
                receiver_pool = cross_border_receivers
            if not receiver_pool:
                raise ValueError(
                    "Aucun destinataire mobile money distinct n'est disponible "
                    f"pour {country}."
                )

            receiver = choose_record_except(
                receiver_pool,
                "customer_id",
                sender["customer_id"],
                random_generator,
            )
            status = random_generator.choices(
                statuses, weights=status_probs, k=1
            )[0]
            amount = round(max(100, random_generator.lognormvariate(10.5, 1.1)), 2)
            sender_country = country
            if (
                unusual_sender_countries
                and random_generator.random()
                < MOBILE_UNUSUAL_COUNTRY_PROBABILITY
            ):
                sender_country = random_generator.choice(
                    unusual_sender_countries
                )

            payment = {
                "payment_id": random_uuid(random_generator),
                "timestamp": random_timestamp(
                    datetime.combine(start_date, time.min),
                    datetime.combine(end_date, time.max),
                    random_generator,
                ),
                "sender_id": sender["customer_id"],
                "receiver_id": receiver["customer_id"],
                "sender_country": sender_country,
                "receiver_country": receiver["country_code"],
                "country_code": country,
                "amount": amount,
                "currency": CURRENCY_MAP[country],
                "operator": random_generator.choice(operators),
                "status": status,
                "fee_amount": round(amount * 0.005, 2) if status == "SUCCESS" else 0.0,
                "entity_type": "MOBILE_MONEY",
            }

            validate(payment, MOBILE_MONEY_SCHEMA, raise_errors=True)
            writer.writerow(
                [
                    payment["payment_id"],
                    payment["timestamp"].isoformat(),
                    payment["sender_id"],
                    payment["receiver_id"],
                    payment["sender_country"],
                    payment["receiver_country"],
                    payment["country_code"],
                    payment["amount"],
                    payment["currency"],
                    payment["operator"],
                    payment["status"],
                    payment["fee_amount"],
                    payment["entity_type"],
                ]
            )

    return output_path


def generate_loan_repayments(
    output_path,
    accounts,
    products,
    country,
    entity_types,
    start_date,
    end_date,
    n_rows=10_000,
    seed=42,
):
    validate_transaction_parameters(country, n_rows)
    validate_period(start_date, end_date)
    allowed_entities = [
        entity for entity in entity_types if entity in {"BANK", "MICROFINANCE"}
    ]
    if not allowed_entities:
        raise ValueError("Les remboursements exigent BANK ou MICROFINANCE.")

    random_generator = random.Random(seed)
    product_by_id = {product["product_id"]: product for product in products}
    loan_accounts = [
        account
        for account in accounts
        if account["country_code"] == country
        and account["entity_type"] in allowed_entities
        and account["account_type"] == "LOAN"
        and account["status"] == "ACTIVE"
        and account["product_id"] in product_by_id
        and product_by_id[account["product_id"]]["loan_type"]
    ]
    if not loan_accounts:
        raise ValueError(f"Aucun compte de prêt actif n'est disponible pour {country}.")

    statuses = ["ON_TIME", "LATE", "DEFAULT"]
    late_probability = 0.12
    default_probability = NPL_TARGETS[country]
    status_probs = [
        1 - late_probability - default_probability,
        late_probability,
        default_probability,
    ]
    output_path = Path(output_path)
    if n_rows <= len(loan_accounts):
        selected_loan_accounts = random_generator.sample(loan_accounts, n_rows)
    else:
        selected_loan_accounts = [
            random_generator.choice(loan_accounts)
            for _ in range(n_rows)
        ]

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(LOAN_REPAYMENTS_COLUMNS)

        for account in selected_loan_accounts:
            product = product_by_id[account["product_id"]]
            status = random_generator.choices(
                statuses, weights=status_probs, k=1
            )[0]
            event_timestamp = random_timestamp(
                datetime.combine(start_date, time.min),
                datetime.combine(end_date, time.max),
                random_generator,
            )
            encours_restant = round(float(account["balance"]), 2)
            amount_due = round(
                min(
                    max(5_000, random_generator.lognormvariate(11.5, 1.0)),
                    encours_restant,
                ),
                2,
            )

            if status == "ON_TIME":
                payment_date = event_timestamp.date()
                due_date = payment_date + timedelta(days=random_generator.randint(0, 5))
                days_overdue = 0
                amount_paid = amount_due
            elif status == "LATE":
                payment_date = event_timestamp.date()
                days_overdue = random_generator.randint(1, 30)
                due_date = payment_date - timedelta(days=days_overdue)
                amount_paid = amount_due
            else:
                payment_date = None
                days_overdue = random_generator.randint(30, 180)
                due_date = event_timestamp.date() - timedelta(days=days_overdue)
                amount_paid = 0.0

            # Variable interne : le schéma source ne contient pas ce montant.
            initial_amount_loan = round(encours_restant + amount_paid, 2)
            if initial_amount_loan < amount_due:
                raise ValueError(
                    "Une échéance de prêt ne peut pas dépasser le montant "
                    "initial du prêt."
                )

            repayment = {
                "repayment_id": random_uuid(random_generator),
                "timestamp": event_timestamp,
                "loan_account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "country_code": country,
                "amount_due": amount_due,
                "amount_paid": amount_paid,
                "currency": account["currency"],
                "due_date": due_date,
                "payment_date": payment_date,
                "days_overdue": days_overdue,
                "loan_type": product["loan_type"],
                "repayment_status": status,
                "entity_type": account["entity_type"],
            }

            validate(repayment, LOAN_REPAYMENTS_SCHEMA, raise_errors=True)
            writer.writerow(
                [
                    repayment["repayment_id"],
                    repayment["timestamp"].isoformat(),
                    repayment["loan_account_id"],
                    repayment["customer_id"],
                    repayment["country_code"],
                    repayment["amount_due"],
                    repayment["amount_paid"],
                    repayment["currency"],
                    repayment["due_date"].isoformat(),
                    repayment["payment_date"].isoformat()
                    if repayment["payment_date"]
                    else "",
                    repayment["days_overdue"],
                    repayment["loan_type"],
                    repayment["repayment_status"],
                    repayment["entity_type"],
                ]
            )

    return output_path

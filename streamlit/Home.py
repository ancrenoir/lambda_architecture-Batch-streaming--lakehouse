import logging
from datetime import date, datetime

import streamlit as st
from dateutil.relativedelta import relativedelta

from scripts.services.service_generation_type_data import (
    COUNTRIES,
    ENTITY_COUNTRIES,
    ENTITY_TYPES,
)
from scripts.simulateur_data_waba import generateur_data_waba


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

DATA_TYPES = [
    "Transactions bancaires",
    "Opérations d'assurance",
    "Paiements mobile money",
    "Remboursements de crédit",
    "Référentiels",
]

REFERENCE_TYPES = ["customers", "products", "branches", "accounts"]

ALLOWED_ENTITIES = {
    "Transactions bancaires": ["BANK", "MICROFINANCE"],
    "Opérations d'assurance": ["INSURANCE"],
    "Paiements mobile money": ["MOBILE_MONEY"],
    "Remboursements de crédit": ["BANK", "MICROFINANCE"],
    "Référentiels": ENTITY_TYPES,
}

DEFAULT_ROWS = {
    "Transactions bancaires": 10_000,
    "Opérations d'assurance": 5_000,
    "Paiements mobile money": 20_000,
    "Remboursements de crédit": 10_000,
    "customers": 500_000,
    "accounts": 800_000,
    "branches": 200,
    "products": 50,
}

DATA_TYPE_CODES = {
    "Transactions bancaires": "bank_transactions",
    "Opérations d'assurance": "insurance_operations",
    "Paiements mobile money": "mobile_money_payments",
    "Remboursements de crédit": "loan_repayments",
}

DATA_TYPE_PATHS = {
    "Transactions bancaires": "bank_transactions",
    "Opérations d'assurance": "insurance_operations",
    "Paiements mobile money": "mobile_money_payments",
    "Remboursements de crédit": "loan_repayments",
}


def run_generation(
    internal_data_type,
    countries,
    entity_types,
    n_rows,
    start_date,
    end_date,
    overwrite,
):
    return generateur_data_waba(
        data_type=internal_data_type,
        countries=countries,
        entity_types=entity_types,
        n_rows=int(n_rows),
        start_date=start_date,
        end_date=end_date,
        overwrite=overwrite,
    )


st.set_page_config(page_title="WABA Data Generator", layout="wide")
st.title("WABA Data Generator")
st.write("Simulation des extractions de données financières du groupe WABA.")

st.subheader("Paramètres de génération")

data_type = st.selectbox(
    "Type de données",
    options=DATA_TYPES,
    index=4,
)

reference_type = None
if data_type == "Référentiels":
    reference_type = st.selectbox(
        "Référentiel",
        options=REFERENCE_TYPES,
        index=0,
    )

entity_options = ALLOWED_ENTITIES[data_type]
entity_types = st.multiselect(
    "Lignes métier",
    options=entity_options,
    default=entity_options,
)

available_countries = [
    country
    for country in COUNTRIES
    if any(country in ENTITY_COUNTRIES[entity] for entity in entity_types)
]

countries = st.multiselect(
    "Pays",
    options=available_countries,
    default=available_countries,
)

volume_key = reference_type if reference_type else data_type
default_rows = DEFAULT_ROWS[volume_key]
step = 10 if volume_key in {"branches", "products"} else 1_000

n_rows = st.number_input(
    "Nombre de lignes",
    min_value=1,
    value=default_rows,
    step=step,
    help=(
        "Pour les flux transactionnels, le volume est appliqué à chaque pays. "
        "Pour un référentiel, il représente le volume total partagé entre les "
        "pays sélectionnés."
    ),
)

is_reference = data_type == "Référentiels"
estimated_total = int(n_rows) if is_reference else int(n_rows) * len(countries)
st.caption(f"Volume total estimé : {estimated_total:,} lignes".replace(",", " "))

today = date.today()
default_start_date = today - relativedelta(months=3)

date_column_start, date_column_end = st.columns(2)
with date_column_start:
    start_date = st.date_input(
        "Date de début",
        value=default_start_date,
        disabled=is_reference,
        help="Période utilisée pour les données transactionnelles.",
    )

with date_column_end:
    end_date = st.date_input(
        "Date de fin",
        value=today,
        disabled=is_reference,
        help="La période par défaut correspond aux trois derniers mois.",
    )

generation_mode = st.radio(
    "Mode de génération",
    options=["One-time"] if is_reference else ["One-time", "Continue"],
    horizontal=True,
    help="Les référentiels sont générés une fois et partagés entre les pays.",
)

generation_interval = None
if generation_mode == "Continue":
    generation_interval = st.slider(
        "Intervalle entre deux générations (secondes)",
        min_value=10,
        max_value=60,
        value=30,
    )

if is_reference:
    destination = f"raw-landing/referentials/{reference_type}.csv"
    internal_data_type = reference_type
else:
    data_path = DATA_TYPE_PATHS[data_type]
    destination = f"raw-landing/<country_code>/{data_path}/<nom_du_fichier>.csv"
    internal_data_type = DATA_TYPE_CODES[data_type]

st.text_input(
    "Destination MinIO",
    value=destination,
    disabled=True,
)

overwrite = False
if is_reference:
    overwrite = st.checkbox(
        "Remplacer le référentiel s'il existe déjà",
        value=False,
        help=(
            "Le remplacement reste désactivé par défaut pour éviter une "
            "opération destructive non contrôlée."
        ),
    )

if reference_type == "accounts":
    st.info(
        "accounts.csv nécessite customers.csv et products.csv déjà présents "
        "dans raw-landing/referentials."
    )
elif data_type == "Transactions bancaires":
    st.info("Ce flux nécessite accounts.csv et branches.csv.")
elif data_type == "Opérations d'assurance":
    st.info("Ce flux nécessite accounts.csv et products.csv.")
elif data_type == "Paiements mobile money":
    st.info("Ce flux nécessite customers.csv.")
elif data_type == "Remboursements de crédit":
    st.info("Ce flux nécessite accounts.csv et products.csv.")

parameters_are_valid = bool(countries and entity_types and start_date <= end_date)

if generation_mode == "One-time":
    submitted = st.button(
        "Générer et envoyer vers MinIO",
        disabled=not parameters_are_valid,
        type="primary",
    )

    if submitted:
        try:
            with st.spinner("Génération et envoi vers MinIO..."):
                result = run_generation(
                    internal_data_type=internal_data_type,
                    countries=countries,
                    entity_types=entity_types,
                    n_rows=n_rows,
                    start_date=start_date,
                    end_date=end_date,
                    overwrite=overwrite,
                )

            st.success("La génération et l'envoi vers MinIO sont terminés.")
            st.json(result)
        except (ValueError, FileExistsError, FileNotFoundError, RuntimeError) as error:
            st.error(str(error))
        except Exception as error:
            logging.exception("data_generation_failed")
            st.error(f"La génération a échoué : {error}")

else:
    if "continuous_running" not in st.session_state:
        st.session_state.continuous_running = False
    if "continuous_history" not in st.session_state:
        st.session_state.continuous_history = []

    start_column, stop_column = st.columns(2)
    with start_column:
        if st.button(
            "Démarrer la génération continue",
            disabled=not parameters_are_valid or st.session_state.continuous_running,
            type="primary",
        ):
            st.session_state.continuous_running = True
    with stop_column:
        if st.button(
            "Arrêter la génération continue",
            disabled=not st.session_state.continuous_running,
        ):
            st.session_state.continuous_running = False

    st.write(
        "État :",
        "en cours" if st.session_state.continuous_running else "arrêté",
    )

    @st.fragment(
        run_every=generation_interval if st.session_state.continuous_running else None
    )
    def continuous_generation():
        if not st.session_state.continuous_running:
            return

        try:
            result = run_generation(
                internal_data_type=internal_data_type,
                countries=countries,
                entity_types=entity_types,
                n_rows=n_rows,
                start_date=start_date,
                end_date=end_date,
                overwrite=False,
            )
            st.session_state.continuous_history.insert(
                0,
                {
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "result": result,
                },
            )
            st.session_state.continuous_history = (
                st.session_state.continuous_history[:10]
            )
            st.success("Nouveau lot généré et envoyé dans MinIO.")
        except Exception as error:
            st.session_state.continuous_running = False
            logging.exception("continuous_generation_failed")
            st.error(f"La génération continue a été arrêtée : {error}")

        if st.session_state.continuous_history:
            st.json(st.session_state.continuous_history)

    continuous_generation()

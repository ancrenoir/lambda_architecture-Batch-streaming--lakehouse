import logging
import os
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.services.service_create_minio_client import create_minio_client
from scripts.services.service_generation_type_data import (
    generate_accounts,
    generate_bank_transactions,
    generate_branches,
    generate_customers,
    generate_insurance_operations,
    generate_loan_repayments,
    generate_mobile_money_payments,
    generate_products,
)
from scripts.services.service_upload_file_to_minio import (
    object_exists,
    read_csv_from_minio,
    upload_file_to_minio,
)


logger = logging.getLogger(__name__)

# Un référentiel inchangé est lu une seule fois par processus Streamlit.
# Son ETag MinIO permet de détecter automatiquement un remplacement.
REFERENCE_CACHE = {}

REFERENCE_OBJECTS = {
    "customers": "referentials/customers.csv",
    "products": "referentials/products.csv",
    "branches": "referentials/branches.csv",
    "accounts": "referentials/accounts.csv",
}

TRANSACTION_NAMES = {
    "bank_transactions": ("bank_transactions", "bank_txn"),
    "insurance_operations": ("insurance_operations", "insurance_ops"),
    "mobile_money_payments": ("mobile_money_payments", "mobile_money"),
    "loan_repayments": ("loan_repayments", "loan_repayments"),
}


def load_reference_records(
    client,
    bucket_name,
    object_name,
    temp_directory,
):
    if not object_exists(client, bucket_name, object_name):
        raise FileNotFoundError(
            f"Le référentiel {bucket_name}/{object_name} n'existe pas."
        )

    object_stat = client.stat_object(bucket_name, object_name)
    cache_key = (bucket_name, object_name)
    cached_reference = REFERENCE_CACHE.get(cache_key)
    if cached_reference and cached_reference["etag"] == object_stat.etag:
        logger.info("reference_cache_hit object=%s", object_name)
        return cached_reference["records"]

    records = read_csv_from_minio(
        client,
        bucket_name,
        object_name,
        temp_directory / Path(object_name).name,
    )
    REFERENCE_CACHE[cache_key] = {
        "etag": object_stat.etag,
        "records": records,
    }
    logger.info(
        "reference_cache_loaded object=%s rows=%s",
        object_name,
        len(records),
    )
    return records


def next_transaction_object_name(
    client,
    bucket_name,
    data_type,
    country,
    file_date,
):
    folder_name, file_prefix = TRANSACTION_NAMES[data_type]
    prefix = f"{country}/{folder_name}/{file_prefix}_{country}_{file_date:%Y%m%d}_"
    existing_sequences = []

    for minio_object in client.list_objects(
        bucket_name,
        prefix=prefix,
        recursive=True,
    ):
        file_name = Path(minio_object.object_name).stem
        sequence_text = file_name.rsplit("_", 1)[-1]
        if sequence_text.isdigit():
            existing_sequences.append(int(sequence_text))

    next_sequence = max(existing_sequences, default=0) + 1
    return f"{prefix}{next_sequence:02d}.csv", next_sequence


def generateur_data_waba(
    data_type,
    countries,
    entity_types,
    n_rows,
    start_date=None,
    end_date=None,
    overwrite=False,
    seed=42,
):
    if data_type not in REFERENCE_OBJECTS and data_type not in TRANSACTION_NAMES:
        raise ValueError(f"Type de données non pris en charge : {data_type}")

    bucket_name = os.environ.get("MINIO_BUCKET", "raw-landing")
    client_minio = create_minio_client()
    end_date = end_date or date.today()
    start_date = start_date or end_date - timedelta(days=90)

    logger.info(
        "generation_started data_type=%s countries=%s entity_types=%s rows=%s",
        data_type,
        countries,
        entity_types,
        n_rows,
    )

    with TemporaryDirectory() as temp_dir:
        temp_directory = Path(temp_dir)

        if data_type in REFERENCE_OBJECTS:
            object_name = REFERENCE_OBJECTS[data_type]
            if object_exists(client_minio, bucket_name, object_name) and not overwrite:
                raise FileExistsError(
                    f"L'objet {bucket_name}/{object_name} existe déjà."
                )

            output_path = temp_directory / f"{data_type}.csv"

            if data_type == "customers":
                generate_customers(
                    output_path=output_path,
                    countries=countries,
                    entity_types=entity_types,
                    n_rows=n_rows,
                    seed=seed,
                    reference_date=end_date,
                )
            elif data_type == "products":
                generate_products(
                    output_path=output_path,
                    countries=countries,
                    entity_types=entity_types,
                    n_rows=n_rows,
                    seed=seed,
                )
            elif data_type == "branches":
                generate_branches(
                    output_path=output_path,
                    countries=countries,
                    entity_types=entity_types,
                    n_rows=n_rows,
                    seed=seed,
                )
            else:
                customers = load_reference_records(
                    client_minio,
                    bucket_name,
                    REFERENCE_OBJECTS["customers"],
                    temp_directory,
                )
                products = load_reference_records(
                    client_minio,
                    bucket_name,
                    REFERENCE_OBJECTS["products"],
                    temp_directory,
                )
                generate_accounts(
                    output_path=output_path,
                    customers=customers,
                    products=products,
                    countries=countries,
                    entity_types=entity_types,
                    n_rows=n_rows,
                    seed=seed,
                    reference_date=end_date,
                )

            upload_result = upload_file_to_minio(
                client=client_minio,
                local_path=output_path,
                bucket_name=bucket_name,
                object_name=object_name,
                overwrite=overwrite,
            )
            REFERENCE_CACHE.pop((bucket_name, object_name), None)
            uploaded_objects = [
                {
                    "object_name": object_name,
                    "country_code": None,
                    "n_rows": n_rows,
                    "etag": upload_result.etag,
                }
            ]

        else:
            accounts = None
            branches = None
            customers = None
            products = None

            if data_type in {
                "bank_transactions",
                "insurance_operations",
                "loan_repayments",
            }:
                accounts = load_reference_records(
                    client_minio,
                    bucket_name,
                    REFERENCE_OBJECTS["accounts"],
                    temp_directory,
                )

            if data_type == "bank_transactions":
                branches = load_reference_records(
                    client_minio,
                    bucket_name,
                    REFERENCE_OBJECTS["branches"],
                    temp_directory,
                )

            if data_type == "mobile_money_payments":
                customers = load_reference_records(
                    client_minio,
                    bucket_name,
                    REFERENCE_OBJECTS["customers"],
                    temp_directory,
                )

            if data_type in {"insurance_operations", "loan_repayments"}:
                products = load_reference_records(
                    client_minio,
                    bucket_name,
                    REFERENCE_OBJECTS["products"],
                    temp_directory,
                )

            uploaded_objects = []
            for country_number, country in enumerate(countries, start=1):
                object_name, sequence = next_transaction_object_name(
                    client_minio,
                    bucket_name,
                    data_type,
                    country,
                    end_date,
                )
                output_path = temp_directory / Path(object_name).name
                generation_seed = seed + sequence + country_number

                if data_type == "bank_transactions":
                    generate_bank_transactions(
                        output_path=output_path,
                        accounts=accounts,
                        branches=branches,
                        country=country,
                        entity_types=entity_types,
                        start_date=start_date,
                        end_date=end_date,
                        n_rows=n_rows,
                        seed=generation_seed,
                    )
                elif data_type == "insurance_operations":
                    generate_insurance_operations(
                        output_path=output_path,
                        accounts=accounts,
                        products=products,
                        country=country,
                        start_date=start_date,
                        end_date=end_date,
                        n_rows=n_rows,
                        seed=generation_seed,
                    )
                elif data_type == "mobile_money_payments":
                    generate_mobile_money_payments(
                        output_path=output_path,
                        customers=customers,
                        country=country,
                        start_date=start_date,
                        end_date=end_date,
                        n_rows=n_rows,
                        seed=generation_seed,
                    )
                else:
                    generate_loan_repayments(
                        output_path=output_path,
                        accounts=accounts,
                        products=products,
                        country=country,
                        entity_types=entity_types,
                        start_date=start_date,
                        end_date=end_date,
                        n_rows=n_rows,
                        seed=generation_seed,
                    )

                upload_result = upload_file_to_minio(
                    client=client_minio,
                    local_path=output_path,
                    bucket_name=bucket_name,
                    object_name=object_name,
                )
                uploaded_objects.append(
                    {
                        "object_name": object_name,
                        "country_code": country,
                        "n_rows": n_rows,
                        "etag": upload_result.etag,
                    }
                )

    total_rows = sum(item["n_rows"] for item in uploaded_objects)
    logger.info(
        "generation_completed data_type=%s objects=%s total_rows=%s",
        data_type,
        len(uploaded_objects),
        total_rows,
    )

    return {
        "bucket_name": bucket_name,
        "data_type": data_type,
        "objects": uploaded_objects,
        "total_rows": total_rows,
    }

import os

from minio import Minio


def create_minio_client():
    required_variables = [
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
    ]
    missing_variables = [
        variable for variable in required_variables if not os.environ.get(variable)
    ]

    if missing_variables:
        raise RuntimeError(
            "Variables MinIO manquantes : " + ", ".join(missing_variables)
        )

    return Minio(
        endpoint=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )

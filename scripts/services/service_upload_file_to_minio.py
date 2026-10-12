import csv
from pathlib import Path

from minio.error import S3Error


def object_exists(client, bucket_name, object_name):
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except S3Error as error:
        if error.code in {"NoSuchKey", "NoSuchObject", "NotFound"}:
            return False
        raise


def upload_file_to_minio(
    client,
    local_path,
    bucket_name,
    object_name,
    overwrite=False,
):
    local_path = Path(local_path)

    if not local_path.is_file():
        raise FileNotFoundError(f"Fichier introuvable : {local_path}")

    if not client.bucket_exists(bucket_name):
        raise RuntimeError(f"Le bucket MinIO {bucket_name} n'existe pas.")

    if object_exists(client, bucket_name, object_name) and not overwrite:
        raise FileExistsError(
            f"L'objet {bucket_name}/{object_name} existe déjà."
        )

    return client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=str(local_path),
        content_type="text/csv",
    )


def read_csv_from_minio(client, bucket_name, object_name, local_path):
    if not object_exists(client, bucket_name, object_name):
        raise FileNotFoundError(
            f"Le référentiel {bucket_name}/{object_name} n'existe pas."
        )

    local_path = Path(local_path)
    client.fget_object(bucket_name, object_name, str(local_path))

    with open(local_path, "r", newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))

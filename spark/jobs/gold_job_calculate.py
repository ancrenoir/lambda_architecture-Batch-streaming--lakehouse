import argparse

from spark.config.gold_kpi_config import GOLD_KPI_CONFIGS
from spark.config.raw_table_configs import COUNTRY_CODES
from spark.services.gold_kpi_assurance import (
    calculer_delai_traitement_sinistres,
    calculer_loss_ratio_par_produit,
)
from spark.services.gold_kpi_banque import (
    calculer_arpu_client_mensuel,
    calculer_ratio_npl_par_pays,
    calculer_volume_transactions_journalier,
)
from spark.services.gold_kpi_mobile_money import (
    calculer_flux_mobile_money_journalier,
    calculer_transferts_transfrontaliers,
)
from spark.services.gold_tables_ecriture import ecrire_table_gold
from spark.services.spark_session import create_spark_session


KPI_CALCULATORS = {
    "daily_transaction_volume": calculer_volume_transactions_journalier,
    "npl_ratio_by_country": calculer_ratio_npl_par_pays,
    "customer_arpu_monthly": calculer_arpu_client_mensuel,
    "loss_ratio_by_product": calculer_loss_ratio_par_produit,
    "claims_processing_time": calculer_delai_traitement_sinistres,
    "mobile_money_daily_flow": calculer_flux_mobile_money_journalier,
    "cross_border_transfers": calculer_transferts_transfrontaliers,
}


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Calcul des KPI Gold.",
    )

    parser.add_argument(
        "--kpi",
        required=True,
        choices=GOLD_KPI_CONFIGS.keys(),
        help="Nom du KPI Gold à calculer.",
    )
    parser.add_argument(
        "--country-code",
        choices=["ALL_COUNTRY", *COUNTRY_CODES],
        default="ALL_COUNTRY",
        help="Code pays à calculer ou ALL_COUNTRY pour tous les pays.",
    )

    return parser.parse_args()


def main():
    args = get_arguments()
    kpi_name = args.kpi
    kpi_config = GOLD_KPI_CONFIGS[kpi_name]
    kpi_calculator = KPI_CALCULATORS[kpi_name]
    country_code = (
        None
        if args.country_code == "ALL_COUNTRY"
        else args.country_code
    )

    spark = create_spark_session(f"gold_job_calculate_{kpi_name}")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS data.gold")

        dataframe = kpi_calculator(
            spark,
            kpi_config,
            country_code,
        )
        ecrire_table_gold(spark, dataframe, kpi_config)

        print(f"KPI Gold créé ou mis à jour : {kpi_config['target_table']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

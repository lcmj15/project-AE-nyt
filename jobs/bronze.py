"""
Job bronze: copia um mês do raw para a bronze, sem alterar o dado.
Uso: python bronze.py --year-month 2026-01 [--base-path /opt/airflow/data]
"""
import argparse
import logging
import os
import re
import sys

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze")


def parse_args():
    parser = argparse.ArgumentParser(description="Carga bronze do NYC Yellow Taxi")
    parser.add_argument("--year-month", required=True, help="Mês no formato AAAA-MM")
    parser.add_argument(
        "--base-path",
        default=os.getenv("DATA_BASE_PATH", "/opt/airflow/data"),
        help="Pasta base dos dados",
    )
    args = parser.parse_args()
    # Falha cedo se o mês vier em formato errado (ex.: 2026-1 ou 2026-13)
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.year_month):
        parser.error(f"--year-month inválido: {args.year_month} (use AAAA-MM)")
    return args


def build_spark():
    return (
        SparkSession.builder
        .appName("bronze_yellow_tripdata")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        # Overwrite só das partições presentes no DataFrame (idempotência)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main():
    args = parse_args()
    ym = args.year_month
    file_name = f"yellow_tripdata_{ym}.parquet"
    raw_path = f"{args.base_path}/raw/{file_name}"
    bronze_path = f"{args.base_path}/bronze/yellow_tripdata"

    # Checa o arquivo ANTES de subir a JVM: erro barato e mensagem clara
    if not os.path.exists(raw_path):
        log.error("Arquivo raw não encontrado: %s", raw_path)
        sys.exit(1)

    spark = build_spark()
    try:
        df_raw = spark.read.parquet(raw_path)

        df_bronze = (
            df_raw
            .withColumn("_source_file", F.lit(file_name))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_year_month", F.lit(ym))
        )

        log.info("Gravando %s em %s", ym, bronze_path)
        df_bronze.write.mode("overwrite").partitionBy("_year_month").parquet(bronze_path)

        # Teste de qualidade: nenhuma linha pode se perder ou duplicar
        n_raw = df_raw.count()
        n_bronze = (
            spark.read.parquet(bronze_path)
            .filter(F.col("_year_month") == ym)
            .count()
        )
        log.info("Contagem raw=%d bronze=%d", n_raw, n_bronze)

        if n_raw != n_bronze:
            raise ValueError(f"Contagem divergente: raw={n_raw} bronze={n_bronze}")

        log.info("Bronze %s OK", ym)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
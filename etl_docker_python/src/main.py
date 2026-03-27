from extract import extract_all
from transform import transform_all
from load import load_data


def run_etl() -> None:
    print("[main] iniciando pipeline ETL...")
    raw_data = extract_all()
    transformed_data = transform_all(raw_data)
    load_data(transformed_data)
    print("[main] pipeline finalizado com sucesso.")


if __name__ == "__main__":
    run_etl()

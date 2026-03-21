from extract import extract_all 
from transform import transform_all
from load import load_all, load_data   

def run_etl() -> None:
    print("[main] Starting ETL process")
    raw_data = extract_all()
    transformed_data = transform_all(raw_data)
    load_data(transformed_data)
    print("[main] ETL process completed")
    
    
if __name__ == "__main__":
    run_etl()   
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.loaders.database import engine


def extract_rh_data(file_path: Path) -> pd.DataFrame:
    return pd.read_excel(file_path)


def transform_rh_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "ID salarié": "employee_id",
            "Nom": "last_name",
            "Prénom": "first_name",
            "Date de naissance": "birth_date",
            "BU": "business_unit",
            "Date d'embauche": "hire_date",
            "Salaire brut": "annual_salary",
            "Type de contrat": "contract_type",
            "Nombre de jours de CP": "leave_days",
            "Adresse du domicile": "home_address",
            "Moyen de déplacement": "transport_mode",
        }
    )


def load_rh_data(df: pd.DataFrame) -> None:
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE bronze.employees;"))

    df.to_sql(
        name="employees",
        schema="bronze",
        con=engine,
        if_exists="append",
        index=False,
    )


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    file_path = project_root / "data" / "raw" / "Données RH.xlsx"

    df = extract_rh_data(file_path)
    df = transform_rh_data(df)
    load_rh_data(df)

    print("Chargement RH terminé avec succès !")


if __name__ == "__main__":
    main()
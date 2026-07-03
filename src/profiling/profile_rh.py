"""
Analyse du fichier RH.

Objectifs :
- Charger le fichier Excel
- Vérifier les types de données
- Identifier les valeurs manquantes
- Détecter les doublons
- Générer un rapport de profiling
"""

import pandas as pd
from pathlib import Path


def load_rh_data(file_path: Path) -> pd.DataFrame:
    """Charge le fichier RH dans un DataFrame Pandas."""
    return pd.read_excel(file_path)


def profile_rh_data(df: pd.DataFrame) -> None:
    """Affiche un rapport simple de profiling des données RH."""
    print("\n==============================")
    print("PROFILING - DONNÉES RH")
    print("==============================")

    print(f"\nNombre de lignes : {df.shape[0]}")
    print(f"Nombre de colonnes : {df.shape[1]}")

    print("\nColonnes :")
    for column in df.columns:
        print(f"- {column}")

    print("\nTypes de données :")
    print(df.dtypes)

    print("\nValeurs manquantes :")
    print(df.isna().sum())

    print("\nDoublons sur l'ensemble des lignes :")
    print(df.duplicated().sum())


def main() -> None:
    """Point d'entrée du script."""
    project_root = Path(__file__).resolve().parents[2]
    file_path = project_root / "data" / "raw" / "Données RH.xlsx"

    df = load_rh_data(file_path)
    profile_rh_data(df)


if __name__ == "__main__":
    main()
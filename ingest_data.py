#!/usr/bin/env python
# coding: utf-8

"""
Script d'ingestion des données NYC Taxi dans PostgreSQL.

Ce script télécharge les données des trajets de taxis jaunes de NYC depuis le dépôt 
DataTalksClub et les insère dans une base de données PostgreSQL en utilisant le 
chargement par chunks pour optimiser l'utilisation de la mémoire.

Utilisation:
    python ingest_data.py --year 2021 --month 1 --target-table yellow_taxi_data
"""

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Définition des types de données pour chaque colonne du dataset NYC Taxi
# Utilisation de Int64 (nullable) plutôt que int64 pour gérer les valeurs manquantes
dtype: dict[str, str] = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# Colonnes à parser en tant que timestamps
parse_dates: list[str] = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--pg-user', default='root', help='Utilisateur PostgreSQL')
@click.option('--pg-pass', default='root', help='Mot de passe PostgreSQL')
@click.option('--pg-host', default='localhost', help='Hôte PostgreSQL')
@click.option('--pg-port', default=5432, type=int, help='Port PostgreSQL')
@click.option('--pg-db', default='ny_taxi', help='Nom de la base de données')
@click.option('--year', default=2021, type=int, help='Année des données')
@click.option('--month', default=1, type=int, help='Mois des données (1-12)')
@click.option('--target-table', default='yellow_taxi_data', help='Nom de la table cible')
@click.option('--chunksize', default=100000, type=int, help='Taille des chunks (nombre de lignes par batch)')
def run(pg_user: str, pg_pass: str, pg_host: str, pg_port: int, pg_db: str, year: int, month: int, target_table: str, chunksize: int) -> None:
    """
    Ingère les données NYC Taxi dans PostgreSQL avec chargement par chunks.
    
    Cette fonction télécharge le fichier CSV compressé depuis le dépôt GitHub,
    le lit par morceaux (chunks) pour optimiser la mémoire, et insère les données
    dans la table PostgreSQL spécifiée.
    
    Args:
        pg_user: Nom d'utilisateur PostgreSQL
        pg_pass: Mot de passe PostgreSQL
        pg_host: Adresse du serveur PostgreSQL
        pg_port: Port PostgreSQL
        pg_db: Nom de la base de données
        year: Année des données à télécharger
        month: Mois des données à télécharger (1-12)
        target_table: Nom de la table où insérer les données
        chunksize: Nombre de lignes à lire par batch (défaut: 100000)
    """
    # Construction de l'URL du fichier CSV à partir de l'année et du mois
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url: str = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    # Création de la connexion SQLAlchemy avec psycopg3
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Lecture du CSV par chunks pour éviter de charger tout le fichier en mémoire
    # iterator=True retourne un itérateur au lieu d'un DataFrame complet
    df_iter: pd.io.parsers.TextFileReader = pd.read_csv(  # type: ignore
        url,
        dtype=dtype,  # type: ignore
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    # Flag pour identifier le premier chunk (création de la table)
    first: bool = True

    # Itération sur chaque chunk avec barre de progression
    for df_chunk in tqdm(df_iter):
        if first:
            # Premier chunk : créer la structure de la table (sans données)
            # head(0) retourne un DataFrame vide avec uniquement les colonnes
            # if_exists='replace' supprime et recrée la table si elle existe déjà
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        # Insérer le chunk dans la table existante
        # if_exists='append' ajoute les données sans recréer la table
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )

if __name__ == '__main__':
    run()  # type: ignore
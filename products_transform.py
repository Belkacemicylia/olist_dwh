import os
import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np


# Configuration de la base de données PostgreSQL
DATABASE_URI = 'postgresql://etl_user:password@localhost:5432/olist_dwh'
engine = create_engine(DATABASE_URI)

# Chemin vers le Data Lake
data_lake_path = "./DataLake"
#  le dossier 'final_data' à la racine du projet qui va contenir les fichier nettoyés
final_data_folder = "./final_data"


# Charger les fichiers CSV
products = pd.read_csv(os.path.join(data_lake_path, "products/olist_products_dataset.csv"))
# Charger les fichiers CSV
order_items = pd.read_csv(os.path.join(data_lake_path, "orders/olist_order_items_dataset.csv"))

# Joindre les deux DataFrames sur la clé étrangère product_id
merged_data = pd.merge(order_items, products, on="product_id", how="inner")
print(len(merged_data))
# Afficher un aperçu des données fusionnées
print(merged_data.head())

# Garder uniquement les colonnes product_id, product_category_name et price
filtered_data = merged_data[["product_id", "product_category_name", "price"]]


# Afficher un aperçu des données filtrées
print(filtered_data.head())

# Sauvegarder les données filtrées dans un fichier CSV pour utilisation future
filtered_data_path = os.path.join(data_lake_path, "filtered_data.csv")
filtered_data.to_csv(filtered_data_path, index=False)


# Charger le fichier CSV filtré
filtered_data = pd.read_csv(filtered_data_path)

# Vérifier les types des colonnes
print(filtered_data.dtypes)
print(filtered_data.shape)

# Examiner le pourcentage de valeurs manquantes dans chaque colonne
missing_percentage = (filtered_data.isna().sum() / len(filtered_data)) * 100

# Afficher les résultats du pourcentage de valeurs manquantes
print("Pourcentage de valeurs manquantes avant remplacement :")
print(missing_percentage)

# Remplacer les cellules vides par NaN
filtered_data.replace("", np.nan, inplace=True)

# Remplacer les valeurs manquantes (NaN) de la colonne 'product_category_name' par 'unknown'
filtered_data['product_category_name'].fillna('unknown', inplace=True)

# Vérifier que les valeurs manquantes ont bien été remplacées
missing_percentage_after = (filtered_data.isna().sum() / len(filtered_data)) * 100
print("Pourcentage de valeurs manquantes après remplacement par la moyenne :")
print(missing_percentage_after)

# Détecter les doublons dans le DataFrame
duplicates = filtered_data.duplicated()

# Afficher le nombre de doublons
num_duplicates = duplicates.sum()
print(f"Nombre de doublons dans le DataFrame : {num_duplicates}")

# Supprimer les doublons
product_trans = filtered_data.drop_duplicates()

# Afficher le nombre de lignes après suppression des doublons
print(f"Nombre de lignes après suppression des doublons : {len(product_trans)}")
duplicates2 = product_trans['product_id'].duplicated()

# Afficher le nombre de doublons
num_duplicates2 = duplicates2.sum()

product_trans2 = filtered_data.drop_duplicates(subset=['product_id'], keep='first')

print(len(product_trans2))

print(filtered_data.shape)
print(product_trans.shape)

# Vérifier si le dossier 'final_data' existe, sinon le créer
if not os.path.exists(final_data_folder):
    os.makedirs(final_data_folder)

# Définir le chemin complet pour le fichier CSV final
output_file_path = os.path.join(final_data_folder, "transformed_products.csv")

# Sauvegarder le DataFrame après toutes les transformations dans un fichier CSV
product_trans2.to_csv(output_file_path, index=False)

# Afficher un message de confirmation
print(f"Le DataFrame transformé a été sauvegardé sous : {output_file_path}")

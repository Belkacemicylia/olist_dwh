import os
import pandas as pd
from sqlalchemy import create_engine, text

# Configuration de la base de données PostgreSQL
DATABASE_URI = 'postgresql://etl_user:password@localhost:5432/olist_dwh'
engine = create_engine(DATABASE_URI)

# Chemin vers le Data Lake
data_lake_path = "./DataLake"
#  le dossier 'final_data' à la racine du projet qui va contenir les fichier nettoyés
final_data_folder = "./final_data"

# Charger les fichiers CSV
products = pd.read_csv(os.path.join(final_data_folder, "transformed_products.csv"))
# Charger les fichiers CSV
sellers = pd.read_csv(os.path.join(final_data_folder, "transformed_sellers.csv"))

# Charger les fichiers CSV
customers = pd.read_csv(os.path.join(final_data_folder, "transformed_customers.csv"))

# Charger les fichiers CSV
date = pd.read_csv(os.path.join(final_data_folder, "dim_time.csv"))


# Charger les fichiers CSV
sales = pd.read_csv(os.path.join(final_data_folder, "fact_sales.csv"))

# Charger les fichiers CSV
orders = pd.read_csv(os.path.join(final_data_folder, "dim_orders.csv"))

# Création des tables dans la base de données PostgreSQL
with engine.connect() as connection:
    # Table des produits
    connection.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id VARCHAR(255) PRIMARY KEY,
            product_category_name VARCHAR(255),
            price FLOAT
        );
    """))
    # Création de la table des vendeurs
    connection.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_sellers (
                seller_id VARCHAR(255) PRIMARY KEY,
                seller_city VARCHAR(255)
            );
        """))
    # Création de la table dim_customers
    connection.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id VARCHAR(255) PRIMARY KEY,
                customer_city VARCHAR(255),
                customer_state VARCHAR(255)
            );
        """))
    # Créer la table dim_time
    connection.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_time (
                date_id VARCHAR(255) PRIMARY KEY,
                order_id VARCHAR(255),
                order_purchase_timestamp TIMESTAMP,
                month INT,
                year INT,
                quarter INT
            );
        """))
    # Création de la table dim_orders
    connection.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_orders (
                    order_id VARCHAR(255) PRIMARY KEY,
                    order_status VARCHAR(50)
                );
            """))

    # Création de la table fact_sales
    connection.execute(text("""
           CREATE TABLE IF NOT EXISTS fact_sales (
               order_id VARCHAR(255) PRIMARY KEY,
               customer_id VARCHAR(255),
               quantity INT,
               product_id VARCHAR(255),
               seller_id VARCHAR(255),
               date_id VARCHAR(255),
               payment_value FLOAT,
               FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
               FOREIGN KEY (order_id) REFERENCES dim_orders(order_id),
               FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
               FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id),
               FOREIGN KEY (date_id) REFERENCES dim_time(date_id)
           );
       """))


# Insérer les données dans les tables PostgreSQL
# Insérer les données dans la table 'dim_products'
products.to_sql('dim_products', engine, if_exists='replace', index=False)
# Insérer les données dans la table 'dim_sellers'
sellers.to_sql('dim_sellers', engine, if_exists='replace', index=False)

# Insérer les données dans la table 'dim_customers'
customers.to_sql('dim_customers', engine, if_exists='replace', index=False)

# Insérer les données dans la table 'dim_time'
date.to_sql('dim_time', engine, if_exists='replace', index=False)

# Insérer les données dans la table 'dim_time'
sales.to_sql('fact_sales', engine, if_exists='replace', index=False)

# Insérer les données dans la table 'dim_time'
orders.to_sql('dim_orders', engine, if_exists='replace', index=False)

print("Les données insérées avec succès dans les tables 'dim_products'.")
print("Les données insérées avec succès dans les tables 'dim_sellers'.")
print("Les données insérées avec succès dans les tables 'dim_customers'.")
print("Les données insérées avec succès dans les tables 'dim_time'.")
print("Les données insérées avec succès dans les tables 'dim_orders'.")
print("Les données insérées avec succès dans les tables 'fact_sales'.")
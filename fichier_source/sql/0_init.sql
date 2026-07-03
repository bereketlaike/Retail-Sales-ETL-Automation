-- Destination table for ETL_analytics_video_games DAG
CREATE TABLE IF NOT EXISTS exploded_products (
      transaction_id TEXT, -- Identifiant unique de la transaction (ticket de caisse)
      store_id TEXT, -- Identifiant du magasin
      store_name TEXT, -- Nom du magasin (ex : “Sports Hub”)
      store_country TEXT, -- Pays dans lequel la transaction a été effectuée
      store_city TEXT, -- Ville du magasin
      store_type TEXT, -- Type de magasin (ex : Mall, Standalone Store)
      purchase_timestamp TIMESTAMP, -- Date et heure de l’achat
      payment_method TEXT, -- Moyen de paiement utilisé (ex : Visa, Cash, Apple Pay...)
      currency TEXT, -- Devise utilisée lors de la transaction (USD, EUR, CAD...)
      total_amount FLOAT, -- Montant total de la transaction (tous produits confondus)
      total_quantity_sold INTEGER, -- Nombre total de produits achetés dans cette transaction
      discount_applied FLOAT, -- Remise totale appliquée à la transaction (en devise locale)
      return_status TEXT, -- Statut du retour : Returned ou Not Returned
      product_id TEXT, -- Identifiant unique du produit vendu
      product_name TEXT, -- Nom du produit (ex : “Running Shoes”)
      product_category TEXT, -- Catégorie du produit (Shoes, Accessories, Equipment…)
      quantity INTEGER, -- Quantité de cet article spécifique dans la transaction
      price FLOAT -- Prix unitaire du produit (ajusté si besoin au coût de la vie par pays)
);

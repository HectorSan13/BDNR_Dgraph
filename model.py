
import datetime
import json


import pydgraph

def set_schema(client):
    schema = """
    # Tipos de Nodos
    type User {
        name
        email
        joined_at
        reviewed
        purchased
        interacted
        has_cart
    }
    
    type Product {
        name
        category
        price
        reviews
        purchased_with
        interactions
    }
    
    type Review {
        rating
        comment
        review_created_at
        reviewed_by
        of_product
    }
    
    type Interaction {
        interaction_type
        timestamp
        duration
        by_user
        with_product
    }
    
    type Cart {
        cart_created_at
        contains
    }
    
    # Índices 
    # User 
    name: string @index(term) .
    email: string @index(exact) .
    joined_at: datetime .
    
    reviewed: [uid] @reverse .
    purchased: [uid] @reverse .
    interacted: [uid] @reverse .
    has_cart: [uid] @reverse .
    
    
    # Product
    category: string @index(exact) .
    price: float @index(float) .
    
    reviews: [uid] @reverse .         
    purchased_with: [uid] @reverse .  
    interactions: [uid] @reverse .    


    # Review
    rating: float @index(float) .
    comment: string @index(fulltext) .
    review_created_at: datetime @index(day) .
    
    reviewed_by: [uid] @reverse .
    of_product: [uid] @reverse .    
    
    # Interaction
    interaction_type: string @index(exact) .
    timestamp: datetime @index(day) .
    duration: float .
    
    by_user: [uid] @reverse .          
    with_product: [uid] @reverse .     
    
    
    # Cart
    cart_created_at: datetime @index(day) .
    contains: [uid] @reverse .       
    """
    return client.alter(pydgraph.Operation(schema=schema))
    
    
# QUERIES
def scalar_map_to_dict(scalar_map):
    """Convierte un ScalarMapContainer o lista de ellos a dicts normales."""
    if isinstance(scalar_map, list):
        return [scalar_map_to_dict(item) for item in scalar_map]
    elif hasattr(scalar_map, "items"):  # es un ScalarMapContainer
        return {k: scalar_map_to_dict(v) for k, v in scalar_map.items()}
    else:
        return scalar_map 

# 3. Obtener reseñas de un producto
def get_reviews(client, product_name):
    txn = client.txn()
    try:
        query = f"""
        {{
            product(func: eq(name, "{product_name}")) {{
                uid
                name
                ~of_product {{
                    rating
                    comment
                    review_created_at
                    reviewed_by {{
                        name
                    }}
                }}
            }}
        }}
        """
        res = txn.query(query)
        data = json.loads(res.json)
        product_data = data.get("product", [])
        reviews = []
        for p in product_data:
            reviews.extend(p.get("~of_product", []))
        return reviews
    finally:
        txn.discard()

# 4. Registro de interacciones (view, click, purchase)
def get_user_interactions(client, user_email):
    txn = client.txn()
    try:
        query = f"""
        {{
            user(func: eq(email, "{user_email}")) {{
                uid
                name
                email
                ~by_user {{
                    uid
                    interaction_type
                    timestamp
                    duration
                    with_product {{
                        uid
                        name
                        category
                        price
                    }}
                }}
            }}
        }}
        """
        res = txn.query(query)
        data = json.loads(res.json)

        user_data = data.get("user", [])
        interactions = []
        for u in user_data:
            interactions.extend(u.get("~by_user", []))

        return interactions
    finally:
        txn.discard()


# 5. Recomendación basada en historial de compras
def get_history_recommendations(client, user_email):
    txn = client.txn()
    try:
        query = f"""
        {{
          user(func: eq(email, "{user_email}")) {{
            name
            ~has_cart {{
              contains {{
                name
                category
                price
              }}
            }}
          }}

          products(func: has(category)) {{
            name
            category
            price
          }}
        }}
        """
        res = txn.query(query)
        data = json.loads(res.json)

        # Productos comprados por el usuario
        purchased = set()
        categories = set()
        for u in data.get("user", []):
            for cart in u.get("~has_cart", []):
                for prod in cart.get("contains", []):
                    purchased.add(prod["name"])
                    categories.add(prod["category"])

        # Filtrar productos similares por categoría
        recommendations = []
        for prod in data.get("products", []):
            if prod["category"] in categories and prod["name"] not in purchased:
                recommendations.append({
                    "name": prod["name"],
                    "category": prod["category"],
                    "price": prod["price"]
                })

        return recommendations
    finally:
        txn.discard()



# 6. Recomendación basada en productos comprados juntos (co-purchase)
def get_copurchased_products(client, product_name):
    txn = client.txn()
    try:
        query = f"""
        {{
            product(func: eq(name, "{product_name}")) {{
                uid
                name
                ~contains {{
                    uid
                    cart_created_at
                    contains {{
                        uid
                        name
                        category
                        price
                    }}
                }}
            }}
        }}
        """
        res = txn.query(query)
        data = json.loads(res.json)

        product_data = data.get("product", [])
        copurchased = {}

        for p in product_data:
            for cart in p.get("~contains", []):
                for other in cart.get("contains", []):
                    if other["name"] != product_name:
                        uid = other["uid"]
                        if uid not in copurchased:
                            copurchased[uid] = {
                                "uid": uid,
                                "name": other["name"],
                                "category": other.get("category"),
                                "price": other.get("price"),
                                "count": 0
                            }
                        copurchased[uid]["count"] += 1

        return sorted(copurchased.values(), key=lambda x: x["count"], reverse=True)
    finally:
        txn.discard()


# Productos Populares
# 7. Más comprados
def get_most_purchased_products(client):
    txn = client.txn()
    try:
        query = """
        {
            interactions(func: eq(interaction_type, "purchase")) {
                interaction_type
                with_product {
                    uid
                    name
                    category
                    price
                }
            }
        }
        """
        res = txn.query(query)
        data = json.loads(res.json)

        product_counts = {}
        for inter in data.get("interactions", []):
            for p in inter.get("with_product", []):
                uid = p["uid"]
                if uid not in product_counts:
                    product_counts[uid] = {
                        "uid": uid,
                        "name": p["name"],
                        "category": p.get("category"),
                        "price": p.get("price"),
                        "purchases": 0
                    }
                product_counts[uid]["purchases"] += 1

        most_purchased = sorted(product_counts.values(), key=lambda x: x["purchases"], reverse=True)
        return most_purchased
    finally:
        txn.discard()


# 8. Más vistos
def get_most_viewed_products(client):
    txn = client.txn()
    try:
        query = """
        {
            interactions(func: eq(interaction_type, "view")) {
                interaction_type
                with_product {
                    uid
                    name
                    category
                    price
                }
            }
        }
        """
        res = txn.query(query)
        data = json.loads(res.json)

        product_counts = {}
        for inter in data.get("interactions", []):
            for p in inter.get("with_product", []):
                uid = p["uid"]
                if uid not in product_counts:
                    product_counts[uid] = {
                        "uid": uid,
                        "name": p["name"],
                        "category": p.get("category"),
                        "price": p.get("price"),
                        "views": 0
                    }
                product_counts[uid]["views"] += 1

        most_viewed = sorted(product_counts.values(), key=lambda x: x["views"], reverse=True)
        return most_viewed
    finally:
        txn.discard()

 

# 9. Recomendación por usuarios similares
def get_similar_users(client, user_email):
    txn = client.txn()
    try:
        # Query para obtener productos del usuario y de todos los demás
        q = f"""
        {{
          user(func: eq(email, "{user_email}")) {{
            name
            ~has_cart {{
              contains {{
                name
                category
                price
              }}
            }}
          }}

          similar(func: has(email)) {{
            name
            email
            ~has_cart {{
              contains {{
                name
                category
                price
              }}
            }}
          }}
        }}
        """
        res = txn.query(q)
        data = json.loads(res.json)

        # Productos del usuario base
        user_data = data.get("user", [])
        purchased = set()
        for u in user_data:
            for cart in u.get("~has_cart", []):
                for prod in cart.get("contains", []):
                    purchased.add(prod["name"])

        # Recomendaciones basadas en usuarios similares
        recommendations = {}
        for su in data.get("similar", []):
            if su.get("email") == user_email:
                continue  # saltar el mismo usuario
            for cart in su.get("~has_cart", []):
                for prod in cart.get("contains", []):
                    if prod["name"] not in purchased:
                        uid = prod["name"]
                        if uid not in recommendations:
                            recommendations[uid] = {
                                "name": prod["name"],
                                "category": prod.get("category"),
                                "price": prod.get("price"),
                                "score": 0
                            }
                        recommendations[uid]["score"] += 1

        return sorted(recommendations.values(), key=lambda x: x["score"], reverse=True)
    finally:
        txn.discard()



# 10. Recomendación por productos top rated
def get_top_rated_products(client):
    txn = client.txn()
    try:
        query = """
        {
            reviews(func: has(rating)) {
                rating
                of_product {
                    uid
                    name
                    category
                    price
                }
            }
        }
        """
        res = txn.query(query)
        data = json.loads(res.json)

        # Diccionario para acumular ratings por producto
        product_ratings = {}
        for r in data.get("reviews", []):
            rating = r.get("rating")
            for p in r.get("of_product", []):
                uid = p["uid"]
                if uid not in product_ratings:
                    product_ratings[uid] = {
                        "uid": uid,
                        "name": p["name"],
                        "category": p.get("category"),
                        "price": p.get("price"),
                        "ratings": []
                    }
                product_ratings[uid]["ratings"].append(rating)

        # Calcular promedio
        top_products = []
        for p in product_ratings.values():
            avg_rating = sum(p["ratings"]) / len(p["ratings"])
            top_products.append({
                "uid": p["uid"],
                "name": p["name"],
                "category": p["category"],
                "price": p["price"],
                "avg_rating": round(avg_rating, 2),
                "num_reviews": len(p["ratings"])
            })

        # Ordenar por promedio descendente
        top_products.sort(key=lambda x: x["avg_rating"], reverse=True)

        return top_products
    finally:
        txn.discard()


# 11. Recomendación por tendencia
def get_trending_products(client):
    txn = client.txn()
    try:
        query = """
        {
            interactions(func: has(interaction_type)) {
                interaction_type
                with_product {
                    uid
                    name
                    category
                    price
                }
            }
        }
        """
        res = txn.query(query)
        data = json.loads(res.json)

        product_counts = {}
        for inter in data.get("interactions", []):
            for p in inter.get("with_product", []): 
                uid = p["uid"]
                if uid not in product_counts:
                    product_counts[uid] = {
                        "uid": uid,
                        "name": p["name"],
                        "category": p.get("category"),
                        "price": p.get("price"),
                        "views": 0,
                        "clicks": 0,
                        "purchases": 0,
                        "total": 0
                    }
                itype = inter.get("interaction_type")
                if itype == "view":
                    product_counts[uid]["views"] += 1
                elif itype == "click":
                    product_counts[uid]["clicks"] += 1
                elif itype == "purchase":
                    product_counts[uid]["purchases"] += 1
                product_counts[uid]["total"] += 1

        trending = sorted(product_counts.values(), key=lambda x: x["total"], reverse=True)
        return trending
    finally:
        txn.discard()


    



    

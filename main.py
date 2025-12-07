import pydgraph
import json
import os

from model import (
    set_schema,
    scalar_map_to_dict,
    get_reviews,
    get_user_interactions,
    get_copurchased_products,
    get_top_rated_products,
    get_trending_products,
    get_most_purchased_products,
    get_most_viewed_products,
    get_similar_users,
    get_history_recommendations
)

from populate import (
    load_users,
    load_products,
    load_reviews,
    load_interactions,
    load_carts
)

# Conexión 
def connect_dgraph():
    client_stub = pydgraph.DgraphClientStub('localhost:9080')
    return pydgraph.DgraphClient(client_stub)

# Reset de datos 
def drop_data(client):
    op = pydgraph.Operation(drop_all=True)
    client.alter(op)
    print("🧹 Datos y Schema borrados.")

# Menú 
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')
    

def print_menu():
    print("══════════════════════════════════════")
    print("               DATABASE               ")
    print("══════════════════════════════════════")
    print("1. Configurar schema")
    print("2. Poblar datos")
    print("3. Obtener reseñas de un producto")
    print("4. Ver interacciones de un usuario")
    print("5. Recomendaciones por historial de compras")
    print("6. Productos comprados juntos (co-purchase)")
    print("7. Productos más comprados")
    print("8. Productos más vistos")
    print("9. Usuarios similares")
    print("10. Productos mejor calificados")
    print("11. Productos en tendencia")
    print("12. Borrar datos")
    print("0. Salir")
    print("══════════════════════════════════════")

def main():
    client = connect_dgraph()

    while True:
        clear_screen()
        print_menu()
        choice = input("👉 Selecciona una opción: ")

        if choice == "1":
            set_schema(client)
            print("✅ Schema configurado correctamente!!")

        elif choice == "2":
            print("📂 Cargando datos...\n")
   
            
            print("Loading USERS...\n")
            users = load_users(client, "data/users.csv")
            user_uid_map = users 
            print(json.dumps(scalar_map_to_dict(user_uid_map), indent=4, ensure_ascii=False))
            print("✔ Users loaded.\n")
            
            print("Loading PRODUCTS...\n")
            products = load_products(client, "data/products.csv")
            product_uid_map = products 
            print(json.dumps(scalar_map_to_dict(product_uid_map), indent=4, ensure_ascii=False))
            print("✔ Products loaded.\n")
            
            print("Loading REVIEWS...\n")
            reviews = load_reviews(client, "data/reviews.csv", user_uid_map, product_uid_map)
            print(json.dumps(scalar_map_to_dict(reviews), indent=4, ensure_ascii=False))
            print("✔ Reviews loaded.\n")
            
            print("Loading INTERACTIONS...\n")
            interactions = load_interactions(client, "data/interactions.csv", user_uid_map, product_uid_map)
            print(json.dumps(scalar_map_to_dict(interactions), indent=4, ensure_ascii=False))
            print("✔ Interactions loaded.\n")
            
            print("Loading CARTS...\n")
            carts = load_carts(client, "data/carts.csv", user_uid_map, product_uid_map)
            print(json.dumps(scalar_map_to_dict(carts), indent=4, ensure_ascii=False))
            print("✔ Carts loaded.\n")

            print("\nDatos poblados correctamente!")

        elif choice == "3":
            product_name = input("Ingrese el nombre EXACTO del producto: ")
            reviews = get_reviews(client, product_name)
            print(f"\nReseñas para el producto: {product_name}\n")
            if not reviews:
                print("No se encontraron reseñas.\n")
            else:
                for i, r in enumerate(reviews, start=1):
                    print(f"Reseña #{i}")
                    print(f"Calificación: {r.get('rating')}")
                    print(f"Comentario: {r.get('comment')}")
                    print(f"Fecha: {r.get('review_created_at')}")
                    reviewers = r.get("reviewed_by", [])
                    if reviewers:
                        for rv in reviewers:
                            print(f"Usuario: {rv.get('name')}")
                    print("-" * 50)


        elif choice == "4":
            email = input("Ingrese EMAIL del usuario: ")
            res = get_user_interactions(client, email)
        
            if not res:
                print(f"\nNo se encontraron interacciones para el usuario con email: {email}\n")
            else:
                print("\nInteracciones:\n")
                for inter in res:
                    products = inter.get("with_product", [])
                    if products:  
                        product = products[0]
                        print(f"- {inter['interaction_type']} en {product.get('name')} "
                              f"(duración: {inter['duration']}s, fecha: {inter['timestamp']})")
                    else:
                        print(f"- {inter['interaction_type']} (sin producto enlazado)")


        elif choice == "5":
            email = input("Ingrese el EMAIL del usuario: ").strip().lower()
            recs = get_history_recommendations(client, email)
            print("\nRecomendaciones basadas en historial de compras: \n")
            if not recs:
                print("No se encontraron recomendaciones.\n")
            else:
                for r in recs[:3]:  # 👈 top 3
                    print(f"- {r['name']} (categoría: {r['category']}, precio: {r['price']})")


        elif choice == "6":
            product_name = input("Ingrese el nombre EXACTO del producto: ")
            res = get_copurchased_products(client, product_name)
            print(f"\nProductos que suelen comprarse junto con {product_name}:\n")
            if not res:
                print("No se encontraron productos copurchased.\n")
            else:
                for r in res[:10]:
                    print(f"- {r['name']} (categoría: {r['category']}, precio: {r['price']}, co-purchase: {r['count']})")


        elif choice == "7":
            res = get_most_purchased_products(client)
            print("\nProductos más comprados:\n")
            if not res:
                print("No se encontraron interacciones de tipo 'purchase'.\n")
            else:
                for p in res[:3]:  # mostrar los 3primeros
                    print(f"- {p['name']} ({p['purchases']} compras, categoría: {p['category']}, precio: {p['price']})")



        elif choice == "8":
            res = get_most_viewed_products(client)
            print("\nProductos más vistos:\n")
            if not res:
                print("No se encontraron interacciones de tipo 'view'.\n")
            else:
                for p in res[:3]:  # mostrar los 3 primeros
                    print(f"- {p['name']} ({p['views']} vistas, categoría: {p['category']}, precio: {p['price']})")


        elif choice == "9":
            email = input("Ingrese el EMAIL del usuario: ").strip().lower()
            recs = get_similar_users(client, email)
            print(f"\nRecomendaciones basadas en usuarios similares para {email}:\n")
            if not recs:
                print("No se encontraron recomendaciones.\n")
            else:
                for r in recs[:3]:
                    print(f"- {r['name']} (categoría: {r['category']}, precio: {r['price']}, score: {r['score']})")


        elif choice == "10":
            res = get_top_rated_products(client)
            print("\nProductos Mejor Calificados:\n")
            for p in res[:5]:  # mostrar los 5 primeros
                print(f"- {p['name']} (⭐ {p['avg_rating']} con {p['num_reviews']} reseñas)")


        elif choice == "11":
            res = get_trending_products(client)
            print("\nProductos en tendencia:\n")
            for p in res[:5]:  # mostrar los 5 primeros
                print(f"- {p['name']} ({p['views']} vistas, {p['clicks']} clicks, {p['purchases']} compras, score popularidad: {p['total']})")


        elif choice == "12":
            drop_data(client)

        elif choice == "0":
            print("\n👋 Saliendo del programa...\n")
            break

        else:
            print("⚠️ Opción inválida.")

        input("\nPresiona ENTER para continuar...")

if __name__ == "__main__":
    main()

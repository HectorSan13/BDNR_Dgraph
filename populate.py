import csv
import pydgraph

# Establish Dgraph client
client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)

def load_users(client,file_path):
    txn = client.txn()
    try:
        users = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                user = {
                    'uid': '_:'+ row['email'].replace(" ", "_"),
                    'name': row['name'],
                    'email': row['email'].strip().lower()
                }
                users.append(user)
        print(f"Loading users: {users}")
        resp = txn.mutate(set_obj = users)
        txn.commit()
    finally:
        txn.discard()
    uid_map = {}
    for original, assigned in resp.uids.items():
        email = original.replace("user_", "")
        uid_map[email] = assigned
    
    return uid_map

def load_products(client, file_path):
    txn = client.txn()
    try:
        products = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                product = {
                    'uid': '_:'+ row['name'].replace(" ", "_"),
                    'name': row['name'],
                    'price': float(row['price']),
                    'category': row['category']
                }
                products.append(product)
        print(f"Loading products: {products}")
        resp = txn.mutate(set_obj = products)
        txn.commit()
    finally:
        txn.discard()
    uid_map = {}
    for original, assigned in resp.uids.items():
        product_name = original.replace("product_", "").replace("_", " ")
        uid_map[product_name] = assigned
    
    return uid_map

def load_reviews(client, file_path, user_uid_map, product_uid_map):
    txn = client.txn()
    try:
        reviews = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:

                uid_name = row['comment'][:20].replace(" ", "_").replace(",", "").replace(".", "")
                
                review = {
                    'uid': f'_:review_{uid_name}',
                    'rating': float(row['rating']),
                    'comment': row['comment'],
                    'review_created_at': row['review_created_at'],
                    'reviewed_by': {'uid': user_uid_map[row['reviewed_by_email']]},
                    'of_product': {'uid': product_uid_map[row['product_name']]}
                }
                reviews.append(review)
        print(f"Loading reviews: {reviews}")
        resp = txn.mutate(set_obj = reviews)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids
    
def load_interactions(client, file_path, user_uid_map, product_uid_map):
    txn = client.txn()
    try:
        interactions = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                uid_base = f"{row['interaction_type']}_{row['timestamp']}".replace(":", "").replace("-", "").replace(".", "")
                
                interaction = {
                    'uid': f'_:interaction_{uid_base}',
                    'interaction_type': row['interaction_type'],
                    'timestamp': row['timestamp'],
                    'duration': float(row['duration']),
                    'by_user': {'uid': user_uid_map[row['user_email'].strip().lower()]},
                    'with_product': {'uid': product_uid_map[row['product_name']]}
                }
                interactions.append(interaction)
        print(f"Loading interactions: {interactions}")
        resp = txn.mutate(set_obj=interactions)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids


def load_carts(client, file_path, user_uid_map, product_uid_map):
    txn = client.txn()
    try:
        carts = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                uid_base = row['cart_created_at'].replace(":", "").replace("-", "").replace(".", "")
                
                cart = {
                    'uid': f'_:cart_{uid_base}',
                    'cart_created_at': row['cart_created_at'],
                    'has_cart': {'uid': user_uid_map[row['user_email'].strip().lower()]},
                    'contains': []
                }
                # productos separados por ;
                products = row['product_name'].split(";")
                for prod in products:
                    cart['contains'].append({'uid': product_uid_map[prod.strip()]})
                
                carts.append(cart)
        print(f"Loading carts: {carts}")
        resp = txn.mutate(set_obj=carts)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids


import csv
import random
from datetime import datetime, timedelta
import os

def generate_customers(num_customers=1000):
    """Generate realistic customer data"""
    first_names = ["John", "Jane", "Michael", "Sarah", "Robert", "Emily", "David", "Emma", "James", "Olivia"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
    
    customers = []
    for i in range(1, num_customers + 1):
        customer = {
            'customer_id': f'CUST{i:06d}',
            'first_name': random.choice(first_names),
            'last_name': random.choice(last_names),
            'email': f'customer{i}@email.com',
            'phone': f'+1{random.randint(2000000000, 9999999999)}',
            'city': random.choice(cities),
            'state': random.choice(states),
            'registration_date': (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d'),
            'loyalty_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'total_spent': round(random.uniform(100, 10000), 2)
        }
        customers.append(customer)
    return customers

def generate_products(num_products=500):
    """Generate realistic product data"""
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Beauty', 'Food']
    brands = ['TechCorp', 'FashionHub', 'HomeStyle', 'SportZone', 'BookWorld', 'ToyLand', 'BeautyPro', 'FoodMart']
    
    products = []
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        product = {
            'product_id': f'PROD{i:06d}',
            'product_name': f'{category} Item {i}',
            'category': category,
            'brand': random.choice(brands),
            'price': round(random.uniform(10, 500), 2),
            'cost': round(random.uniform(5, 250), 2),
            'stock_quantity': random.randint(0, 1000),
            'supplier_id': f'SUPP{random.randint(1, 50):03d}',
            'weight': round(random.uniform(0.1, 50), 2),
            'is_active': random.choice(['true', 'false'])
        }
        products.append(product)
    return products

def generate_orders(customers, products, num_orders=5000):
    """Generate realistic order data with order items"""
    orders = []
    order_items = []
    
    for i in range(1, num_orders + 1):
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        customer = random.choice(customers)
        
        order = {
            'order_id': f'ORD{i:08d}',
            'customer_id': customer['customer_id'],
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'status': random.choice(['Completed', 'Processing', 'Shipped', 'Cancelled']),
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash']),
            'shipping_address': f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Park'])} St",
            'shipping_city': customer['city'],
            'shipping_state': customer['state'],
            'discount_amount': round(random.uniform(0, 50), 2)
        }
        
        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        order_total = 0
        
        for j in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 10)
            unit_price = float(product['price'])
            line_total = round(quantity * unit_price, 2)
            order_total += line_total
            
            order_item = {
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': line_total,
                'discount_percent': random.choice([0, 5, 10, 15, 20])
            }
            order_items.append(order_item)
        
        order['total_amount'] = round(order_total - float(order['discount_amount']), 2)
        orders.append(order)
    
    return orders, order_items

def save_to_csv(data, filename):
    """Save data to CSV file"""
    if not data:
        return
    
    output_path = os.path.join('..', 'data', 'raw', filename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Generated {filename} with {len(data)} records")

def main():
    print("Generating sample retail data...")
    
    # Generate data
    customers = generate_customers(1000)
    products = generate_products(500)
    orders, order_items = generate_orders(customers, products, 5000)
    
    # Save to CSV files
    save_to_csv(customers, 'customers.csv')
    save_to_csv(products, 'products.csv')
    save_to_csv(orders, 'orders.csv')
    save_to_csv(order_items, 'order_items.csv')
    
    print("\nSample data generation completed!")
    print(f"Total customers: {len(customers)}")
    print(f"Total products: {len(products)}")
    print(f"Total orders: {len(orders)}")
    print(f"Total order items: {len(order_items)}")

if __name__ == "__main__":
    main()
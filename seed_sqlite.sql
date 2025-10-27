-- ============================================================
-- Café Demo Dataset — COMPLETE (SQLite)
-- Covers 2023–2025 with daily orders and items
-- ============================================================

PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

-- Core tables
CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY,
  customer_name TEXT
);

CREATE TABLE products (
  product_id INTEGER PRIMARY KEY,
  product_name TEXT,
  category TEXT,
  unit_price REAL
);

CREATE TABLE orders (
  order_id    INTEGER PRIMARY KEY,    -- ROWID alias
  customer_id INTEGER NOT NULL,
  order_date  TEXT NOT NULL,          -- YYYY-MM-DD
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  order_item_id INTEGER PRIMARY KEY,  -- ROWID alias
  order_id      INTEGER NOT NULL,
  product_id    INTEGER NOT NULL,
  quantity      INTEGER NOT NULL,
  line_total    REAL NOT NULL,
  FOREIGN KEY (order_id)   REFERENCES orders(order_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE INDEX idx_orders_order_date ON orders(order_date);

-- Dimensions
INSERT INTO customers (customer_id, customer_name) VALUES
 (1,'Alice'),(2,'Ben'),(3,'Carlos'),(4,'Dana'),
 (5,'Ethan'),(6,'Fiona'),(7,'Grace'),(8,'Henry'),
 (9,'Irene'),(10,'Jack'),(11,'Kim'),(12,'Leo');

INSERT INTO products VALUES
 (1,'Espresso 3oz','Coffee',80.0),
 (2,'Americano','Coffee',110.0),
 (3,'Cappuccino','Coffee',140.0),
 (4,'Latte','Coffee',150.0),
 (5,'Mocha','Coffee',165.0),
 (6,'Cold Brew','Coffee',160.0),
 (7,'Matcha Latte','Tea',170.0),
 (8,'English Breakfast Tea','Tea',120.0),
 (9,'Lemon Iced Tea','Beverages',95.0),
 (10,'Fresh Orange Juice','Beverages',130.0),
 (11,'Butter Croissant','Pastries',90.0),
 (12,'Chocolate Croissant','Pastries',110.0),
 (13,'Blueberry Muffin','Pastries',95.0),
 (14,'Bagel with Cream Cheese','Breakfast',120.0),
 (15,'Caesar Salad','Salads',180.0),
 (16,'Chicken Panini','Sandwiches',220.0),
 (17,'Tuna Melt','Sandwiches',210.0),
 (18,'Margherita Flatbread','Mains',250.0),
 (19,'Carbonara Pasta','Mains',280.0),
 (20,'Classic Cheesecake','Desserts',160.0);

-- A few hand-entered example orders/items (optional variety)
INSERT INTO orders (order_id, customer_id, order_date) VALUES
 (101,1,'2024-01-15'),
 (102,5,'2024-02-10'),
 (103,9,'2024-03-05'),
 (110,4,'2024-11-11'),
 (111,8,'2024-12-22'),
 (201,1,'2025-10-20'),
 (202,2,'2025-10-21'),
 (203,3,'2025-10-22');

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, line_total) VALUES
 (3001,101,4,1,150.0),(3002,101,11,1, 90.0),
 (3003,102,2,2,220.0),(3004,102,20,1,160.0),
 (3005,103,1,2,160.0),
 (3019,110,2,1,110.0),(3020,110,15,1,180.0),(3021,110,10,1,130.0),
 (3022,111,6,2,320.0),(3023,111,20,1,160.0);

-- ============================================================
-- Daily coverage 2023-01-01 .. 2025-12-31
-- ============================================================
WITH RECURSIVE cal(d) AS (
  SELECT date('2023-01-01')
  UNION ALL
  SELECT date(d, '+1 day') FROM cal WHERE d < date('2025-12-31')
)
-- Insert one order per day if missing
INSERT INTO orders (customer_id, order_date)
SELECT ((CAST(strftime('%j', d) AS INT) - 1) % 12) + 1 AS customer_id,
       d
FROM cal
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.order_date = cal.d);

-- Insert at least one beverage line for each order in range
INSERT INTO order_items (order_id, product_id, quantity, line_total)
SELECT
  o.order_id,
  CASE WHEN CAST(strftime('%d', o.order_date) AS INT) % 2 = 0 THEN 2 ELSE 4 END AS product_id, -- Americano or Latte
  CASE WHEN CAST(strftime('%d', o.order_date) AS INT) % 3 = 0 THEN 2 ELSE 1 END AS qty,
  (SELECT unit_price FROM products
     WHERE product_id = (CASE WHEN CAST(strftime('%d', o.order_date) AS INT) % 2 = 0 THEN 2 ELSE 4 END))
  * (CASE WHEN CAST(strftime('%d', o.order_date) AS INT) % 3 = 0 THEN 2 ELSE 1 END) AS line_total
FROM orders o
WHERE o.order_date BETWEEN '2023-01-01' AND '2025-12-31'
  AND NOT EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.order_id);

-- Add a food line when the order has only one line
INSERT INTO order_items (order_id, product_id, quantity, line_total)
SELECT
  o.order_id,
  CASE WHEN CAST(strftime('%m', o.order_date) AS INT) % 2 = 0 THEN 11 ELSE 16 END AS product_id, -- Croissant or Panini
  1 AS qty,
  (SELECT unit_price FROM products
     WHERE product_id = (CASE WHEN CAST(strftime('%m', o.order_date) AS INT) % 2 = 0 THEN 11 ELSE 16 END)) AS line_total
FROM orders o
WHERE o.order_date BETWEEN '2023-01-01' AND '2025-12-31'
  AND (SELECT COUNT(*) FROM order_items x WHERE x.order_id = o.order_id) = 1;

-- Sanity checks (uncomment to test in sqlite shell)
-- SELECT MIN(order_date), MAX(order_date) FROM orders;
-- SELECT strftime('%Y', order_date) AS y, COUNT(*) AS days FROM orders GROUP BY y;

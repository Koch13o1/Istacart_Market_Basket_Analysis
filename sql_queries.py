"""
Atharva Chiplunkar
Vaishnavi Patil
Maitreya Bhupesh Kocharekar
"""

import psycopg2
import time
#import pandas as pd

def main():
    conn = psycopg2.connect(database="project", host="localhost", user="maitreyakocharekar", password="project123", port="5432")
    cursor = conn.cursor()
    q1(cursor)
    q2(cursor)
    q3(cursor)
    q4(cursor)
    q5(cursor)
    createIndexes(cursor)
    print("After creating required indexes: ")
    q1(cursor)
    q2(cursor)
    q3(cursor)
    q4(cursor)
    q5(cursor)

def q1(cursor):
    start = time.time()
    sql_query= """
    SELECT p.product_id, p.product_name, COUNT(op.product_id) AS reorder_count
    FROM product p
    JOIN order_product op ON p.product_id = op.product_id
    JOIN orders o ON op.order_id = o.order_id
    WHERE op.reorder = 1
    GROUP BY p.product_id,p.product_name
    ORDER BY reorder_count DESC
    LIMIT 10;
    """
    cursor.execute(sql_query)
    end = time.time()
    print("Time taken by query1 : ", end-start,"s")




def q2(cursor):
    start = time.time()
    sql_query= """
    SELECT d.department_id, d.department_name, AVG(o.days_since_prior_order) AS avg_days_since_last_order
    FROM department d
    JOIN product p ON d.department_id = p.department_id
    JOIN order_product op ON p.product_id = op.product_id
    JOIN orders o ON op.order_id = o.order_id
    GROUP BY d.department_id, d.department_name
    ORDER BY avg_days_since_last_order ASC;
    """
    cursor.execute(sql_query)
    end = time.time()
    print("Time taken by query2 : ", end-start,"s")

def q3(cursor):
    start = time.time()
    sql_query= """
    SELECT a.aisle_id, a.aisle_name, COUNT(CASE WHEN op.reorder = 1 THEN 1 END) AS total_reordered,                                                                                                             COUNT(*) AS total_ordered,
    (COUNT(CASE WHEN op.reorder = 1 THEN 1 END)* 100)/COUNT(*) AS reorder_percentage 
    FROM aisle a 
    JOIN product p ON a.aisle_id = p.aisle_id 
    JOIN order_product op ON p.product_id = op.product_id 
    GROUP BY a.aisle_id,a.aisle_name
    ORDER BY reorder_percentage DESC
    LIMIT 10;
    """
    cursor.execute(sql_query)
    end = time.time()
    print("Time taken by query3 : ", end-start,"s")

def q4(cursor):
    start = time.time()
    sql_query= """
    SELECT d.department_id, d.department_name, o.order_hour_of_day, COUNT(op.product_id) AS product_count
    FROM department d
    JOIN product p ON d.department_id = p.department_id
    JOIN order_product op ON p.product_id = op.product_id
    JOIN orders o ON op.order_id = o.order_id
    GROUP BY d.department_id, d.department_name, o.order_hour_of_day
    ORDER BY d.department_id, o.order_hour_of_day;
    """
    cursor.execute(sql_query)
    end = time.time()
    print("Time taken by query4 : ", end-start,"s")



def q5(cursor):
    start = time.time()
    sql_query= """
    SELECT p1.product_name AS product_1, p2.product_name AS product_2, COUNT(*) AS pair_count
    FROM order_product op1
    JOIN order_product op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id
    JOIN product p1 ON op1.product_id = p1.product_id
    JOIN product p2 ON op2.product_id = p2.product_id
    GROUP BY p1.product_id,p1.product_name, p2.product_id, p2.product_name
    ORDER BY pair_count DESC
    LIMIT 10;
    """
    cursor.execute(sql_query)
    end = time.time()
    print("Time taken by query5 : ", end-start,"s")

def createIndexes(cursor):
    sql_query = """
    CREATE INDEX idx_order_product_product_id_reorder ON order_product (product_id, reorder);
    CREATE INDEX idx_orders_order_id ON orders (order_id);
    CREATE INDEX idx_product_product_id ON product (product_id);
    CREATE INDEX idx_department_id ON department (department_id);
    CREATE INDEX idx_product_department_id ON product (department_id);
    CREATE INDEX idx_order_product_product_id ON order_product (product_id);
    CREATE INDEX idx_aisle_id ON aisle (aisle_id);
    CREATE INDEX idx_product_aisle_id ON product (aisle_id);
    
    CREATE INDEX idx_order_product_reorder ON order_product (reorder);
    
    CREATE INDEX idx_orders_order_hour_of_day ON orders (order_hour_of_day);
    
    CREATE INDEX idx_order_product_order_id_product_id ON order_product (order_id, product_id);
    
    """
    cursor.execute(sql_query)



if __name__ == "__main__":
    main()
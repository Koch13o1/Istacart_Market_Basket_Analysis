"""
Maitreya Bhupesh Kocharekar
Atharva Chiplunkar
Vaishnavi Patil
"""
import psycopg2
import time
#import pandas as pd



def main():
    s_t = time.time()
    conn = psycopg2.connect(database="project", host="localhost", user="maitreyakocharekar", password="project123", port="5432")
    print(conn)
    cursor = conn.cursor()

    sql_query_1 = """
    CREATE TABLE orders(
    order_id int,
    user_id int,
    order_number int,
    order_dow int,
    order_hour_of_day int,
    days_since_prior_order int
    );

    CREATE TABLE product(
    product_id int,
    product_name text,
    aisle_id int,
    department_id int
    );
    
    CREATE TABLE aisle(
    aisle_id int,
    aisle_name text
    );
    
    CREATE TABLE department(
    department_id int,
    department_name text
    );
    
    CREATE TABLE order_product(
    order_id int,
    product_id int,
    add_to_cart_order int,
    reorder int
    );
    
    """

    cursor.execute(sql_query_1)
    sql_query_2 = """
    CREATE TABLE users(
    user_id int,
    name varchar(1000)
    );
    """
    cursor.execute(sql_query_2)
    conn.commit()


if __name__ == "__main__":
    main()
import psycopg2
import time
import pandas as pd

def main():
    conn = psycopg2.connect(database="project", host="localhost", user="maitreyakocharekar", password="project123", port="5432")
    cursor = conn.cursor()
    sql_query = """
    CREATE VIEW view1 AS
    SELECT d.department_name, a.aisle_name
    FROM product p
    JOIN department d ON d.department_id = p.department_id
    JOIN aisle a ON a.aisle_id = p.aisle_id;
    
    CREATE VIEW view2 AS 
    SELECT distinct department_name, array_agg(distinct aisle_name) AS aisles 
    FROM view1 
    GROUP BY department_name 
    ORDER BY department_name ASC;
    
    CREATE VIEW view3 AS 
    SELECT a.aisle_name, p.product_name 
    FROM product p 
    JOIN aisle a ON a.aisle_id = p.aisle_id;
    
    CREATE VIEW aisle_product AS 
    SELECT DISTINCT aisle_name, array_agg(DISTINCT product_name) 
    FROM view3 GROUP BY aisle_name 
    ORDER BY aisle_name ASC;
    
    """
    cursor.execute(sql_query)

    sql_query_2 = """
    
    """
    conn.commit()

if __name__ == "__main__":
    main()
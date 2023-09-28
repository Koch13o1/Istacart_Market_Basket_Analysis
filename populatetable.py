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
    """
    pd1 = pd.read_csv(r'/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/project/instacart-market-basket-analysis/orders.csv')
    print(pd1.head())
    pd1['days_since_prior_order'] = pd1['days_since_prior_order'].fillna(0)
    pd1['days_since_prior_order'] = pd1['days_since_prior_order'].astype('int')
    pd1 = pd1.loc[pd1['eval_set'] == 'prior']
    orders = pd1[["order_id","user_id","order_number", "order_dow", "order_hour_of_day","days_since_prior_order"]]
    orders.to_csv('/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/project/instacart-market-basket-analysis/ordersnew.csv', encoding ='utf-8', index = False)
    """



    sql_query = """
    CREATE TABLE temp(
    order_id int,
    user_id int,
    eval_set text,
    order_number int,
    order_dow int,
    order_hour_of_day int,
    days_since_prior_order float
    );
    
    copy temp
    from '/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/project/instacart-market-basket-analysis/orders.csv'
    DELIMITER ','
    CSV HEADER;
    
    ALTER TABLE orders ALTER COLUMN days_since_prior_order TYPE float;

    INSERT INTO orders
    SELECT order_id, user_id, order_number,order_dow,order_hour_of_day,days_since_prior_order
    FROM temp
    WHERE temp.eval_set = 'prior';

    """

    cursor.execute(sql_query)
    sql_query2 = """
    copy aisle
    from '/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/project/instacart-market-basket-analysis/aisles.csv'
    DELIMITER ','
    CSV HEADER;
    
    
    copy department
    from '/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/project/instacart-market-basket-analysis/departments.csv'
    DELIMITER ','
    CSV HEADER;
    
    copy order_product
    from '/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/project/instacart-market-basket-analysis/order_products__prior.csv'
    DELIMITER ','
    CSV HEADER;
    
    copy product
    from '/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/project/instacart-market-basket-analysis/products.csv'
    DELIMITER ','
    CSV HEADER;
    
    """

    cursor.execute(sql_query2)
    sql_query3 = """
    copy users
    from '/Users/maitreyakocharekar/Documents/Sem2(Spring2022-23)/Big Data/BDproject/venv/user.csv'
    DELIMITER ','
    CSV HEADER;
    """
    cursor.execute(sql_query3)
    conn.commit()



if __name__ == "__main__":
    main()


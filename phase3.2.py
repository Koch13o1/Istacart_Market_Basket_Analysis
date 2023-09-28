import psycopg2
import time
import pandas as pd

def main():
    conn = psycopg2.connect(database="project", host="localhost", user="maitreyakocharekar", password="project123", port="5432")
    cursor = conn.cursor()
    sql_query = """
    CREATE TABLE L3P(
    id int,
    name varchar(1000));
    
    SELECT product_id1, product_id2, product_id3 FROM L3;
    """
    start = time.time()
    cursor.execute(sql_query)
    lst= []
    for row in cursor:
        print(row)
        row1 = row[0:8]
        print(row1)
        row1 = list(row1)
        lst = lst + row1
    s_list = list(set(lst))
    print("s_list: ", s_list)
    sql_query_2 = """
    INSERT INTO L3P(id)
    """
    for i in range(len(s_list)):
        cursor.execute(sql_query_2 + "VALUES(" + str(s_list[i]) + "); ")
    sql_query_3 = """
    CREATE TABLE CommonItems AS
    SELECT L3P.ID AS ID, PRODUCT.product_name AS NAME
    FROM L3P
    JOIN PRODUCT ON L3P.ID = PRODUCT.product_id
    """

    cursor.execute(sql_query_3)
    #for row in cursor:
        #print(row)
    end = time.time()
    print("Time taken by Q5b: ", end - start, " s")
    conn.commit()



if __name__ == "__main__":
    main()


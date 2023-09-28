i=1
line3 = "FROM temp2 pma1 "
lastline = "HAVING COUNT(DISTINCT pma1.order_id) >=10000 ORDER BY COUNT(pma1.order_id) DESC;"
while(i<5):
    if i>1:
        line1 = "CREATE TABLE L" + str(i) +" AS "
        line2 = "SELECT "
        for x in range(1,i+1):
            line2 = line2 + "pma" + str(x) + ".product_id as product_id" + str(x) + ", "
        line2 = line2 + "COUNT(DISTINCT pma1.order_id) as count "
        line4 = ""
        for x in range(1,i):
            line4_1 = "JOIN temp2 pma"
            line4 = line4 + line4_1 + str(x+1) + " ON " + "pma1.order_id = pma" + str(x+1) + ".order_id AND pma" + str(x) + ".product_id < pma" + str(x+1) + ".product_id "
        line5 = "GROUP BY pma1.product_id"
        extra_line = "JOIN L" + str(i-1) + " ON "
        for x in range(1,i):
            if x< i-1:
                extra_line = extra_line + "pma" + str(x) + ".product_id = L" + str(i-1) + ".product_id" + str(x) + " AND "
            else:
                extra_line = extra_line + "pma" + str(x) + ".product_id = L" + str(i-1) + ".product_id" + str(x) + " "
        extra_line1 = "WHERE "
        for x in range(1, i):
            if x< i-1:
                extra_line1 = extra_line1 + "pma" + str(i) + ".product_id IN (SELECT product_id" + str(x) + " FROM L" + str(i-1) + ") OR "
            else:
                extra_line1 = extra_line1 + "pma" + str(i) + ".product_id IN (SELECT product_id" + str(x) + " FROM L" + str(i-1) + ") "

        for x in range(1,i):
            line5 = line5 + ", pma" + str(x+1) + ".product_id "
        total = line1 + line2 + line3 + line4 + extra_line + extra_line1 + line5 + lastline
    else:
        total = "CREATE TABLE L1 AS SELECT product_id as product_id1, COUNT(DISTINCT order_id) as count FROM temp2 GROUP BY product_id HAVING COUNT(DISTINCT order_id) >= 10000 ORDER BY COUNT(order_id) DESC;"
    print(total)
    i += 1



Bag of Organic Bananas | Organic Strawberries | Organic Hass Avocado | 15066

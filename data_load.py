import ibm_db_dbi as db  # provides connection to IBM DB
import os  # provides access to system variables
import random

conn = None
resultSet = False

# retrieve user_name, password and host values from the system variables:
user_name = os.environ['USER_NAME']
password = os.environ['PASSWORD']
host = os.environ['HOST']


# provides connection
try:
    conn = db.connect("DATABASE=BLUDB;"
                      "HOSTNAME={host};"
                      "PORT=50000;"
                      "PROTOCOL=TCPIP;"
                      "UID={user_name};"
                      "PWD={password};".format(host=host, user_name=user_name, password=password), "", "")
    cur = conn.cursor()
    print("\nConnection successfully established.\n")
except Exception:
    print("\nERROR: Unable to connect to the BLUDB database. Check credentials.")
    exit(-1)

# Drop table year_sales if exists:
try:
    cur.execute('DROP TABLE year_sales;')
    conn.commit()
    print("\nTable was successfully deleted.\n")
except Exception:
    print("\nTable year_sales hasn't existed yet.\n")


# CREATE new year_sales table:
try:
    cur.execute('''CREATE TABLE year_sales (product_id INT NOT NULL,
                                            product_group_id INT NOT NULL,
                                            year INT NOT NULL,
                                            amount_month_1 INT NOT NULL,
                                            amount_month_2 INT NOT NULL,
                                            amount_month_3 INT NOT NULL,
                                            amount_month_4 INT NOT NULL,
                                            amount_month_5 INT NOT NULL,
                                            amount_month_6 INT NOT NULL,
                                            amount_month_7 INT NOT NULL,
                                            amount_month_8 INT NOT NULL,
                                            amount_month_9 INT NOT NULL,
                                            amount_month_10 INT NOT NULL,
                                            amount_month_11 INT NOT NULL,
                                            amount_month_12 INT NOT NULL,
                                            PRIMARY KEY (product_id, year)
                                        );''')
    conn.commit()
    print("\nTable 'year_sales' was successfully created.\n")
except Exception:
    print("\nERROR: Unable to execute the SQL statement specified.\n")
    conn.close()
    exit(-1)


# function which make a list of rows with random values
def rows_lst(num_rows):

    rows, pr_group, pr_year = [], {}, {}
    while len(rows) < num_rows:
        # generate product_id
        product_id = random.randint(1, 20000)
        # generate year
        year = random.randint(2015, 2018)
        try:
            # check if this product/year have been already used
            if year in pr_year.get(product_id):
                # if so - go to next iteration
                continue
            else:
                # if no - append year to value list
                pr_year[product_id].append(year)
        except TypeError:
            # if this key hasn't been used, insert new product_id: [year]
            pr_year.setdefault(product_id, []).append(year)
        # check if product_id is in pr_group dct and if no, add rand  value
        pr_group[product_id] = pr_group.get(product_id, random.randint(1, 9))
        # get a row f rand values
        row = [product_id, pr_group[product_id], year, *[random.randint(1, 100000) for _ in range(12)]]
        # add new row to rows
        rows.append(row)
    return rows

# indicate the required number of rows
values_for_insert = rows_lst(20000)
print("\nList of strings created successfully.\n")

# We found out that batches of 800 records are successfully processed by the cur.executemany () operation.
# And batches of 900 records can cause errors.
# Let's use batches of 500 records, because:
# 1. It's more convenient with round numbers (like 1000, 13000 or 10000000).
# 2. So as not to worry that mistakes can happen.

try:
    chunks = (len(values_for_insert) - 1) // 500 + 1
    for i in range(chunks):
        batch = values_for_insert[i*500:(i+1)*500]
        cur.executemany('''INSERT INTO year_sales(product_id,
                                                  product_group_id,
                                                  year,
                                                  amount_month_1,
                                                  amount_month_2,
                                                  amount_month_3,
                                                  amount_month_4,
                                                  amount_month_5,
                                                  amount_month_6,
                                                  amount_month_7,
                                                  amount_month_8,
                                                  amount_month_9,
                                                  amount_month_10,
                                                  amount_month_11,
                                                  amount_month_12)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', batch)
        conn.commit()
    print("\n" + str(len(values_for_insert)) + " rows was successfully created.\n")
except Exception:
    print("\nERROR: Unable to execute the SQL statement specified.\n")
    conn.close()
    exit(-1)
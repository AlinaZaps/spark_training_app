import ibm_db_dbi as db  # provides connection to IBM DB
import os  # provides access to system variables
import time
import random

# retrieve user_name, password and host values from the system variables:
user_name = os.environ['DB2_USER']
password = os.environ['DB2_PASSWORD']
host = os.environ['DB2_HOST']


# provides connection
def conn_to_db():
    try:
        conn = db.connect("DATABASE=BLUDB;"
                          "HOSTNAME={host};"
                          "PORT=50000;"
                          "PROTOCOL=TCPIP;"
                          "UID={user_name};"
                          "PWD={password};".format(host=host, user_name=user_name, password=password), "", "")
        cur = conn.cursor()
        print("\nConnection successfully established.\n")
        return conn, cur
    except Exception:
        print("\nERROR: Unable to connect to the BLUDB database. Check credentials.")
        exit(-1)


def check_table(conn, curs):
    # Drop table year_sales if exists:
    try:
        cur.execute('DROP TABLE year_sales;')
        conn.commit()
        print("\nTable was successfully deleted.\n")
        return None
    except Exception:
        print("\nTable year_sales hasn't existed yet.\n")


# CREATE new year_sales table:
def create_table(conn, cur):
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
        print("\nERROR: Unable to execute the CREATing.\n")
        conn.close()
        exit(-1)


# function which make a list of rows with random values
def rows_lst(num_rows):
    rows, pr_group, pr_year = [], {}, {}
    while len(rows) < num_rows:
        # generate product_id
        product_id = random.randint(1, 100000)
        # generate year
        year = random.randint(1900, 2021)
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
    print("created " + str(num_rows) + " rows.")
    return rows


# indicate the required number of rows

# We found out that batches of 800 records are successfully processed by the cur.executemany () operation.
# And batches of 900 records can cause errors.
# Let's use batches of 500 records, because:
# 1. It's more convenient with round numbers (like 1000, 13000 or 10000000).
# 2. So as not to worry that mistakes can happen.


def insert_data(conn, cur, values_for_insert):
    # try:
    chunks = (len(values_for_insert) - 1) // 500 + 1
    for i in range(chunks):
        batch = values_for_insert[i * 500:(i + 1) * 500]
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
    return "\n" + str(len(values_for_insert)) + " rows was successfully inserted.\n"
    # except Exception:
    #     print("\nERROR: Unable to execute the INSERTing.\n")
    #     conn.close()
    #     exit(-1)


if __name__ == "__main__":
    start = time.time()
    connection, cursor = conn_to_db()
    # check_table(connection, cursor)
    create_table(connection, cursor)
    insert_vals = rows_lst(3000000)
    print(insert_data(connection, cursor, insert_vals))

    end = time.time()
    print('TASK DURATION is ' + str(end - start))


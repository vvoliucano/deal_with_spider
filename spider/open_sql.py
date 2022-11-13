import sqlite3
import os
import pandas as pd


def read_from_database(name):
    cnt_tables = 0
    with sqlite3.connect(f'./database/{name}/{name}.sqlite') as con:
        con.text_factory = lambda b: b.decode(errors = 'ignore')
        c = con.cursor()
    # c.execute("SELECT * FROM Activity")
    sql_query = """SELECT name FROM sqlite_master
    WHERE type='table';"""
    c.execute(sql_query)
    table_name_list = c.fetchall()

    dir_path = f'data_csv/{name}'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    def export_table_as_csv(table_name):
        sql_query = f'SELECT * FROM {table_name}'
        c.execute(sql_query)
        col_name_list = [item[0] for item in c.description]
        items = c.fetchall()
        data = pd.DataFrame(columns = col_name_list, data = items)
        file_path = os.path.join(dir_path, f'{table_name}.csv')
        data.to_csv(file_path, encoding='utf-8', index = False)

        if len(items):
            return True
        return False

    for table_name in table_name_list:
        if export_table_as_csv(table_name[0]):
            cnt_tables += 1

    return cnt_tables


def main():
    cnt_tables = 0
    for db_name in os.listdir('./database'):
        print(db_name)
        cnt_tables += read_from_database(db_name)

    print(cnt_tables)

main()

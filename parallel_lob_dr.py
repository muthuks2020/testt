import datetime
import subprocess as sb
import psycopg2 as pg2
import concurrent.futures
import time

def open_db_conn(dbname, user, host, password):
    conn_string = "dbname=" + dbname + " user=" + user + " host=" + host + " password=" + password
    conn = pg2.connect(conn_string)
    print("connected to db: ", host)
    return conn

def check_for_lob(names_list, source_host, source_user, source_password):
    lob_table_list = []
    for dbname in names_list:
        # check_connection(dbname, source_user, source_host, source_password)
        conn = open_db_conn(dbname, source_user, source_host, source_password)
        cur = conn.cursor()
        find_lob_table_name = """
            select distinct(table_schema, table_name)
            from information_schema.columns
            where table_schema not in ('information_schema', 'pg_catalog','pglogical') and data_type in ('oid');
        """
        cur.execute(find_lob_table_name)
        results = cur.fetchall()
        cur.close()
        conn.close()
        #[schema.table]
        #[jan.image04, public.image04]
        if results != []:
            for result in results:
                result = result[0].split(',')
                schema = result[0].split('(')[1]
                table = result[1].split(')')[0]
                lob_table_list.append(schema + "." + table)
        print(lob_table_list)
    return lob_table_list

def dump_data(conn, lob_oid):
    if type(lob_oid) is not str and lob_oid is not None:
        try:
            start_time = time.time()
            with open('/opt/backup/' + str(lob_oid), 'wb') as file:
                file.write(conn.lobject(oid=lob_oid, mode="rb").read())
            end_time = time.time()
            print(f"Dump completed for {lob_oid} in {round(end_time - start_time)} seconds")
        except Exception as e:
            print("Exception during dump: ", e)
            raise e

def restore_data(conn, lob_oid, count):
    if type(lob_oid) is not str and lob_oid is not None:
        try:
            start_time = time.time()
            cur = conn.cursor()
            new_lob = conn.lobject(mode="wb", new_file="/opt/backup/" + str(lob_oid), new_oid=lob_oid)
            cur.execute(update_lob(lob_oid, new_lob.oid))
            print("old lob: ", lob_oid)
            print("new_lob: ", new_lob.oid)
            conn.commit()
            end_time = time.time()
            count += 1
            print(f"Restore completed for {lob_oid} in {round(end_time - start_time)} seconds")
        except Exception as e:
            print("Exception during restore: ", e)
            raise e

    # if count == 100:
    #     conn.commit()
    #     count = 0

def get_column_with_oid(conn, lob_table):
    columns_with_oid = []
    schema = "'" + str(lob_table.split('.')[0]) + "'"
    table = "'" + str(lob_table.split('.')[1]) + "'"
    fetch_columns_with_oid = f"SELECT column_name FROM information_schema.columns where table_name={table} and table_schema={schema} and data_type='oid';"
    cur = conn.cursor()
    print(fetch_columns_with_oid)
    try:
        cur.execute(fetch_columns_with_oid)
        results = cur.fetchall()
        if results:
            for result in results:
                columns_with_oid.append(result[0] + ",")
            return columns_with_oid
        else:
            return []
    except Exception as e:
        print("Exception while fetching columns with oid: ",e)
        raise e

def get_lob_oid_list(conn, lob_table):
    columns_with_oid = get_column_with_oid(conn, lob_table) #oid => LOB

    lob_oid_list = []
    fetch_column_data_with_schema = "SELECT "

    if columns_with_oid:
        for i in range(len(columns_with_oid)):
            if i != len(columns_with_oid) - 1:
                fetch_column_data_with_schema = fetch_column_data_with_schema + f"{columns_with_oid[i]} "
            else:
                column = columns_with_oid[i].split(',')[0]
                fetch_column_data_with_schema = fetch_column_data_with_schema + f"{column} "

        fetch_column_data_with_schema = fetch_column_data_with_schema + f" FROM {lob_table};"
        #SELECT lobcol1, lobcol2 from public.image04;
        cur = conn.cursor()
        try:
            cur.execute(fetch_column_data_with_schema)
            results = cur.fetchall()
            for result in results:
                for data in result:
                    lob_oid_list.append(data)
            print(lob_oid_list)
        except Exception as e:
            print("Exception while fetching lob oid list: ", e)
            raise e
    return lob_oid_list

def dump_oid_list(conn, lob_table, lob_oid_list):

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(dump_data, conn, lob_oid) for lob_oid in lob_oid_list}
        
    for fut in concurrent.futures.as_completed(futures):
            result = fut.result()
    end = time.time()
    print(f"Dump finished for {lob_table} in {round(end-start_time, 2)} second(s)")

def update_lob(old_oid, new_oid):
    return f"UPDATE pg_largeobject SET loid={old_oid} where loid={new_oid}; UPDATE pg_largeobject_metadata SET oid = {old_oid} where oid = {new_oid};"

def restore_oid_list(conn, lob_table, lob_oid_list):
    count = 0
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(restore_data, conn, lob_oid, count) for lob_oid in lob_oid_list}
        
    for fut in concurrent.futures.as_completed(futures):
            result = fut.result()
    end = time.time()
    print(f"Restore finished for {lob_table} in {round(end-start_time, 2)} second(s)")


def dump_and_restore(source_database, source_user, source_host, source_password, destination_database, destination_user, destination_host, destination_password, lob_table):

    source_conn = open_db_conn(source_database, source_user, source_host, source_password)

    destination_conn = open_db_conn(destination_database, destination_user, destination_host, destination_password)

    lob_oid_list = get_lob_oid_list(source_conn, lob_table)

    if lob_oid_list:
        dump_oid_list(source_conn, lob_table, lob_oid_list)

        restore_oid_list(destination_conn, lob_table, lob_oid_list)
    else:
        return


def main(source_database, source_user, source_host, source_password, destination_database, destination_user, destination_host, destination_password, lob_table_list):

    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = {executor.submit(dump_and_restore, source_database, 
                                                            source_user, 
                                                            source_host, 
                                                            source_password, 
                                                            destination_database, 
                                                            destination_user, 
                                                            destination_host, 
                                                            destination_password,
                                                            lob_table
            ) for lob_table in lob_table_list}
        
    for fut in concurrent.futures.as_completed(futures):
            result = fut.result()
    end = time.time()
    print(f"Dump and Restore for databases finished in {round(end-start_time, 2)} second(s)")



if __name__ == "__main__":
    source_user = "postgres" #input("Enter source user")
    source_password = "postgres" #4input("Enter source password")
    source_host = "34.70.35.87" #input("Enter source host")
    source_database = "calnder" #input("Enter source database")
    destination_user = "postgres" #input("Enter source user")
    destination_password = "postgres" #input("Enter source password")
    destination_host = "35.188.171.197" #input("Enter source host")
    destination_database = "calnder" #input("Enter source database")

    lob_table_list = check_for_lob([source_database], source_host, source_user, source_password)

    if lob_table_list:
        main(source_database, 
            source_user, 
            source_host, 
            source_password, 
            destination_database, 
            destination_user, 
            destination_host, 
            destination_password,
            lob_table_list
        )
    else:
        print("No lob tables present in the database")

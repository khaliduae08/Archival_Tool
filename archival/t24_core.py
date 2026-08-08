import logging
import math
import uuid
import threading
from datetime import datetime
import pyodbc
from .models import AuditLog, T24Table, ArchivalTransaction, ArchivalTransactionDetail
from .job_tracker import update_progress


logger = logging.getLogger(__name__)

CHUNK_SIZE = 2000000  


def build_conn_str(db_conn):
    """Build pyodbc connection string from DatabaseConnection instance."""
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_conn.server};"
        f"DATABASE={db_conn.database};"
        f"UID={db_conn.username};"
        f"PWD={db_conn.password};"
    )


def fetch_bulk(select_query, src_conn_str, date_str,
               key_id, no_of_sel, sel_ind, temp_table_name):
    
    conn = None
    try:
        conn = pyodbc.connect(src_conn_str)

        conn.cursor().execute(f"TRUNCATE TABLE {temp_table_name}")

        if "where" in select_query.lower():
            final_query = (
                f"{select_query} AND "
                f"ABS(CAST(HASHBYTES('MD5', CAST(s.{key_id} AS VARCHAR(20))) AS BIGINT)) "
                f"% {no_of_sel} = {sel_ind}"
            )
        else:
            final_query = (
                f"{select_query} WHERE "
                f"ABS(CAST(HASHBYTES('MD5', CAST(s.{key_id} AS VARCHAR(20))) AS BIGINT)) "
                f"% {no_of_sel} = {sel_ind}"
            )

        final_query = final_query.replace("{archival_date}", date_str)
        final_query = f"INSERT INTO {temp_table_name} {final_query}"

        print(f"[chunk-{sel_ind}/{no_of_sel}] {final_query}")
        conn.cursor().execute(final_query)
        conn.commit()
        print(f"[chunk-{sel_ind}/{no_of_sel}] Fetch done.")

    except Exception as e:
        print(f"[chunk-{sel_ind}] Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def process_bulk(insert_query, delete_query, dstn_conn_str, src_conn_str, dstn_table,
                 temp_table_name, shard_idx, total_shards, total_inserted, total_deleted,
                 ins_del_lock,user=None):
    conn = None
    conn_src = None
    try:
        conn = pyodbc.connect(dstn_conn_str)        
        cur = conn.cursor()        

        insert_filter = (   
            f"% {total_shards} = {shard_idx}"
        )
        
        final_insert = insert_query.replace("{ids}", f"{temp_table_name}") 
        final_insert = final_insert.replace("{filter}", insert_filter)
        print(final_insert)      
        

        # if delete_query is not None:
            
        # print (f"[consumer-{shard_idx}] {final_delete}")


        print(f"[consumer-{shard_idx}] Inserting shard {shard_idx}...")
        cur.execute(final_insert)
        conn.commit()
        with ins_del_lock:
            total_inserted[0] += cur.rowcount

        if delete_query is not None:
            conn_src = pyodbc.connect(src_conn_str)
            cur_src = conn_src.cursor()
            
            delete_filter = (
                f"ABS(CAST(HASHBYTES('MD5', CAST(RECID AS VARCHAR(20))) AS BIGINT)) "
                f"% {total_shards} = {shard_idx}"
            )
            
            final_delete = delete_query.replace(
                "{ids}",
                f"SELECT RECID FROM {temp_table_name} "
                f"WHERE {delete_filter}"
            )
            print(f"[consumer-{shard_idx}] Deleting shard {shard_idx}...")

            del_script = f"DELETE FROM {temp_table_name} WHERE NOT EXISTS (SELECT 1 FROM {dstn_table} dst WHERE dst.RECID = {temp_table_name}.RECID)"
            print(f"{del_script}")
            cur_src.execute(del_script)
            # conn_src.commit()

            if "TOP" in delete_query:
                while True:
                    cur_src.execute(final_delete)
                    n=cur_src.rowcount
                    conn_src.commit()
                    with ins_del_lock:
                        total_deleted[0] += cur_src.rowcount
                    if n == 0:
                        break
            else:
                cur_src.execute(final_delete)
                conn_src.commit()
                with ins_del_lock:
                    total_deleted[0] += cur_src.rowcount

        print(f"[consumer-{shard_idx}] Done.")
        return

    except Exception as e:
        print(f"[consumer-{shard_idx}] Error: {e}")
        AuditLog.objects.create(
            user=user,
            action='PROCESS',
            module=dstn_table,
            details=f"Error archiving T24 table {dstn_table}: {str(e)}",
            success=False
        )
        if conn:
            conn.rollback()
        if conn_src:
            conn_src.rollback()
    finally:
        if conn:
            conn.close()
        if conn_src:
            conn_src.close()


def run_t24_table(table_id, row_count, user, job_id=None):
    temp_table_name = None
    src_conn_str = None
    start_time = datetime.now()

    try:
        table          = table_id
        application    = table.application
        select_session = table.select_session
        insert_session = table.insert_session
        batch_size     = table.batch_size

        select_qry = table.select_script
        insert_qry = table.insert_script
        delete_qry = table.delete_script

        date_str = table.archival_date.strftime('%Y%m%d')
        print(f"Starting archival for table {table.table_name} with date {date_str}...")

        src_conn_str  = build_conn_str(application.src_conn)
        dstn_conn_str = build_conn_str(application.dstn_conn)

        key_id = 'RECID'

    
        try:
            row_count = int(row_count)
        except (TypeError, ValueError):
            row_count = 0

        sel_cnt = max(1, math.ceil(row_count / CHUNK_SIZE)) if row_count else 1
        print(f"[main] row_count={row_count} → splitting into {sel_cnt} chunk(s) "
              f"of ~{CHUNK_SIZE:,} rows each.")

        temp_table_name = f"{table.table_name.strip()}_{uuid.uuid4().hex}"
        print(f"[main] Creating temp table {temp_table_name}...")

        setup_conn = pyodbc.connect(src_conn_str)
        setup_conn.cursor().execute(f"""
            CREATE TABLE {temp_table_name} (
                RECID NVARCHAR(500) PRIMARY KEY NONCLUSTERED
            );
            CREATE CLUSTERED INDEX IX_{temp_table_name}_RECID
                ON {temp_table_name}(RECID);
        """)
        setup_conn.commit()
        setup_conn.close()
        print(f"[main] Temp table created.")

        total_inserted = [0]
        total_deleted  = [0]
        ins_del_lock   = threading.Lock()
    
        for sel_ind in range(sel_cnt):
            print(f"[main] === Processing chunk {sel_ind + 1}/{sel_cnt} ===")

            fetch_bulk(select_qry, src_conn_str, date_str,
                       key_id, sel_cnt, sel_ind, temp_table_name)

            consumer_threads = []
            for shard_idx in range(insert_session):
                ct = threading.Thread(
                    target=process_bulk,
                    args=(insert_qry, delete_qry, dstn_conn_str, src_conn_str, table.table_name,
                          temp_table_name, shard_idx, insert_session,
                          total_inserted, total_deleted, ins_del_lock,user),
                    name=f"consumer-{shard_idx}",
                    daemon=True,
                )
                consumer_threads.append(ct)
                ct.start()

            for ct in consumer_threads:
                ct.join()

            print(f"[main] Chunk {sel_ind + 1}/{sel_cnt} done. "
                  f"Running totals → inserted: {total_inserted[0]}, deleted: {total_deleted[0]}")
            
            if job_id:
                update_progress(job_id, {
                    'inserted': total_inserted[0],
                    'deleted': total_deleted[0],
                    'chunk': sel_ind + 1,
                    'total_chunks': sel_cnt
                })
            
        execution_time = datetime.now() - start_time
        execution_seconds = execution_time.total_seconds()
        

        transaction = ArchivalTransaction.objects.create(
            userName=user,
            archival_date=table.archival_date,
            total_execution_time=execution_seconds,
            module_id=2
        )

        ArchivalTransactionDetail.objects.create(
            transaction=transaction,
            table_name=table,
            total_rows_inserted=total_inserted[0],
            total_rows_deleted=total_deleted[0],
            total_rows_archived=row_count,
            status='success',
            execution_time=execution_seconds
        )

        AuditLog.objects.create(
            user=user,
            action='PROCESS',
            module=table.table_name,
            details=f"Inserted {total_inserted[0]} and Deleted {total_deleted[0]} from {table.table_name}",
            success=True
        )

        return {
            'status': 'success',
            'rows_inserted': total_inserted[0],
            'rows_deleted': total_deleted[0],
            'table': table.table_name
        }

    except Exception as e:
        logger.error(f"Error running T24 archival for table {table_id}: {e}")
        execution_time = datetime.now() - start_time
        execution_seconds = execution_time.total_seconds()
        transaction = ArchivalTransaction.objects.create(
            userName=user,
            archival_date=table.archival_date,
            total_execution_time=execution_seconds,
            module_id=2
        )

        ArchivalTransactionDetail.objects.create(
            transaction=transaction,
            table_name=table,
            total_rows_inserted=total_inserted[0],
            total_rows_deleted=total_deleted[0],
            total_rows_archived=row_count,
            status='error',
            error_message=str(e),
        )

        AuditLog.objects.create(
            user=user,
            action='PROCESS',
            module=table.table_name,
            details=f"Error archiving T24 table {table.table_name}: {str(e)}",
            success=False
        )
        return {'status': 'error', 'error': str(e)}

    finally:
        if temp_table_name and src_conn_str:
            try:
                cleanup_conn = pyodbc.connect(src_conn_str)
                cleanup_conn.cursor().execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                cleanup_conn.commit()
                cleanup_conn.close()
                print(f"[main] Temp table {temp_table_name} dropped.")
            except Exception as ce:
                logger.error(f"Failed to drop temp table {temp_table_name}: {ce}")
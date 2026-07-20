import logging
import uuid
import threading
import queue
import pyodbc

from .models import AuditLog, T24Table
XML_TYPE = -152
logger = logging.getLogger(__name__)

SENTINEL = None  # signals consumers that producers are done

def prepare_batch(rows):
    """Convert XML string column to bytes for SQL Server XML type."""
    result = []
    for row in rows:
        recid  = row[0]
        pdate  = row[1]
        xmlval = row[2]
        
        # Encode XML string as UTF-16-LE bytes — what SQL Server XML type expects
        if isinstance(xmlval, str):
            xml_bytes = xmlval.encode('utf-16-le')
        elif xmlval is None:
            xml_bytes = None
        else:
            xml_bytes = xmlval  # already bytes

        result.append((recid, pdate, xml_bytes))
    return result

def build_conn_str(db_conn):
    """Build pyodbc connection string from DatabaseConnection instance."""
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_conn.server};"
        f"DATABASE={db_conn.database};"
        f"UID={db_conn.username};"
        f"PWD={db_conn.password};"
    )

def connect_with_xml_support(conn_str):
    conn = pyodbc.connect(conn_str)
    
    def xml_converter(raw_bytes):
        if raw_bytes is None:
            return None
        decoded = raw_bytes.decode('utf-16-le')
        return decoded.lstrip('\ufeff')  
    
    conn.add_output_converter(XML_TYPE, xml_converter)
    return conn


def validate_insert(src_conn_str, dstn_conn_str,dstn_table, temp_table_name, 
                    insert_query, total_inserted):

    src_conn  = None
    dstn_conn = None
    try:
        # Count in temp table (what we selected)
        src_conn = pyodbc.connect(src_conn_str)
        src_cur  = src_conn.cursor()
        src_cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
        temp_count = src_cur.fetchone()[0]


        # Count in destination (what was actually inserted)
        dstn_conn = pyodbc.connect(dstn_conn_str)
        dstn_cur  = dstn_conn.cursor()
        dstn_cur.execute(f"""
            SELECT COUNT(*) FROM {dstn_table} 
            WHERE RECID IN (SELECT RECID FROM {temp_table_name})
        """)
        
        print(f"[validate] Temp table count : {temp_count}")
        print(f"[validate] Total inserted   : {total_inserted[0]}")

        if temp_count == total_inserted[0]:
            print(f"[validate] ✓ Counts match — safe to delete.")
            return True
        else:
            print(f"[validate] ✗ Mismatch — skipping delete to prevent data loss.")
            logger.error(
                f"[validate] Count mismatch: temp={temp_count}, inserted={total_inserted[0]}"
            )
            return False

    except Exception as e:
        print(f"[validate] Error during validation: {e}")
        logger.error(f"[validate] Error: {e}")
        return False
    finally:
        if src_conn:
            src_conn.close()
        if dstn_conn:
            dstn_conn.close()

def fetch_bulk(select_query, src_conn_str, date_str,
               key_id, no_of_sel, sel_ind, temp_table_name,
               batch_size, data_queue):
    conn = None
    try:
        # conn = connect_with_xml_support(src_conn_str)
        # cur  = conn.cursor()
        conn = pyodbc.connect(src_conn_str)
        cur  = conn.cursor()

        if "where" in select_query.lower():
            shard_query = (
                f"{select_query} AND "
                f"ABS(CAST(HASHBYTES('MD5', CAST({key_id} AS VARCHAR(20))) AS BIGINT)) "
                f"% {no_of_sel} = {sel_ind} "
            )
        else:
            shard_query = (
                f"{select_query} WHERE "
                f"ABS(CAST(HASHBYTES('MD5', CAST({key_id} AS VARCHAR(20))) AS BIGINT)) "
                f"% {no_of_sel} = {sel_ind} "
            )

        shard_query = shard_query.replace("{archival_date}", date_str)
        insert_temp  = f"INSERT INTO {temp_table_name} SELECT RECID FROM ({shard_query}) AS sub OPTION (MAXDOP 8, RECOMPILE)"

        # print(f"[producer-{sel_ind}] Populating temp table: {insert_temp}")
        cur.execute(insert_temp)
        conn.commit()
        print(f"[producer-{sel_ind}] Temp table shard inserted. Rows: {cur.rowcount}")

        # fetch_query = f"""
        #     SELECT RECID,
        #            XMLRECORD.value('(/row/c25/text())[1]', 'nvarchar(8)') AS PDATE,
        #            XMLRECORD
        #     FROM FKMB_CATEG_ENTRY WITH (NOLOCK)
        #     WHERE RECID IN (
        #         SELECT RECID FROM {temp_table_name}
        #         WHERE ABS(CAST(HASHBYTES('MD5', CAST(RECID AS VARCHAR(20))) AS BIGINT))
        #         % {no_of_sel} = {sel_ind}
        #     )
        #     OPTION (MAXDOP 8, RECOMPILE)
        # """

        # print(f"[producer-{sel_ind}] Fetching rows from source...")
        cur.execute(shard_query)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break

            batch = [tuple(row) for row in rows]
            print(f"[producer-{sel_ind}] Fetching batch of {len(rows)} rows.")
            data_queue.put(batch)
            print(f"[producer-{sel_ind}] Queued batch of {len(rows)} rows. "
                  f"Queue size: {data_queue.qsize()}")

        print(f"[producer-{sel_ind}] Done fetching.")

    except Exception as e:
        print(f"[producer-{sel_ind}] Error: {e}")
        logger.error(f"[producer-{sel_ind}] Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def insert_bulk(insert_query,  dstn_conn_str,
                data_queue, consumer_idx, temp_table_name, total_inserted, insert_lock):
    conn = None
    # total_inserted = 0
    # insert_query = f"{insert_query}"
    try:
        conn = pyodbc.connect(dstn_conn_str)
        cur  = conn.cursor()
        cur.fast_executemany = False  

        while True:
            batch = data_queue.get()
            if batch is SENTINEL:
                data_queue.task_done()
                break            
            
            try:

                recid_list = ', '.join(f"''{recid[0]}''" for recid in batch)
    
                # final_query = insert_query.replace("{ids}", "?")
                
                cur.execute(insert_query,recid_list)
                conn.commit()
                with insert_lock:
                    total_inserted[0] += len(batch)
                print(f"[consumer-{consumer_idx}] Inserted {len(batch)} rows "
                      f"(total: {total_inserted}). Queue: {data_queue.qsize()}")
            except Exception as batch_err:
                conn.rollback()
                print(f"[consumer-{consumer_idx}] Batch error: {batch_err}")
                logger.error(f"[consumer-{consumer_idx}] Batch error: {batch_err}")
            finally:
                data_queue.task_done()

        print(f"[consumer-{consumer_idx}] Done. Total inserted: {total_inserted}")

    except Exception as e:
        print(f"[consumer-{consumer_idx}] Fatal error: {e}")
        logger.error(f"[consumer-{consumer_idx}] Fatal error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def delete_bulk(delete_query, src_conn_str, temp_table_name, dstn_table,
                shard_idx, total_shards):
    
    conn = None
    try:
        conn = pyodbc.connect(src_conn_str)
        cur  = conn.cursor()

        shard_filter = (
            f"SELECT RECID FROM {temp_table_name} WHERE EXISTS(SELECT 1 FROM {dstn_table} dst WHERE dst.RECID = {temp_table_name}.RECID)"
            f" AND ABS(CAST(HASHBYTES('MD5', CAST(RECID AS VARCHAR(20))) AS BIGINT)) "
            f"% {total_shards} = {shard_idx}"
        )
        final_delete = delete_query.replace("IN ({ids})", f"IN ({shard_filter})")

        print(f"[delete-{shard_idx}] Running delete...")
        cur.execute(final_delete)
        conn.commit()
        print(f"[delete-{shard_idx}] Deleted {cur.rowcount} rows.")

    except Exception as e:
        print(f"[delete-{shard_idx}] Error: {e}")
        logger.error(f"[delete-{shard_idx}] Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def run_t24_table(table_id, user):
    temp_table_name = None
    src_conn_str    = None

    try:
        table          = table_id
        application    = table.application
        select_session = table.select_session   # number of producer threads
        insert_session = table.insert_session   # number of consumer threads
        batch_size     = table.batch_size       # rows per batch

        select_qry = table.select_script
        insert_qry = table.insert_script        # e.g. INSERT INTO FKMB_CATEG_ENTRY#RO VALUES (?,?,?)
        delete_qry = table.delete_script

        date_str      = application.max_date.strftime('%Y%m%d')
        src_conn_str  = build_conn_str(application.src_conn)
        dstn_conn_str = build_conn_str(application.dstn_conn)

        # print(f"Source DB : {application.src_conn.server}/{application.src_conn.database}")
        # print(f"Dest DB   : {application.dstn_conn.server}/{application.dstn_conn.database}")

        key_id = 'RECID'

        temp_table_name = f"{table.table_name.strip()}_{uuid.uuid4().hex}"
        # print(f"[main] Creating temp table {temp_table_name}...")

        setup_conn = pyodbc.connect(src_conn_str)
        setup_conn.cursor().execute(f"""
            CREATE TABLE {temp_table_name} (
                RECID NVARCHAR(100) PRIMARY KEY NONCLUSTERED
            );
            CREATE CLUSTERED INDEX IX_{temp_table_name}_RECID
                ON {temp_table_name}(RECID);
        """)
        setup_conn.commit()
        setup_conn.close()
        print(f"[main] Temp table created.")

        data_queue = queue.Queue(maxsize=insert_session * 4)

        total_inserted = [0]
        insert_lock    = threading.Lock()
        
        consumer_threads = []
        for idx in range(insert_session):
            ct = threading.Thread(
                target=insert_bulk,
                args=(insert_qry, dstn_conn_str,
                      data_queue, idx,temp_table_name, total_inserted, insert_lock),
                name=f"consumer-{idx}",
                daemon=True,
            )
            consumer_threads.append(ct)
            ct.start()

        producer_threads = []
        for sel_ind in range(select_session):
            pt = threading.Thread(
                target=fetch_bulk,
                args=(select_qry, src_conn_str, date_str,
                      key_id, select_session, sel_ind,
                      temp_table_name, batch_size, data_queue),
                name=f"producer-{sel_ind}",
                daemon=True,
            )
            producer_threads.append(pt)
            pt.start()

        for pt in producer_threads:
            pt.join()
        print("[main] All producers done.")

        for _ in range(insert_session):
            data_queue.put(SENTINEL)

        for ct in consumer_threads:
            ct.join()
        print(f"[main] All consumers done. Total inserted: {total_inserted[0]}")

        is_valid = validate_insert(
            src_conn_str, dstn_conn_str,table,
            temp_table_name, insert_qry, total_inserted
        )

        if not is_valid:
            logger.error(
                f"Validation failed for {table.table_name} — "
                f"delete skipped. Manual intervention required."
            )
            AuditLog.objects.create(
                user=user,
                action='Run T24 Table',
                details=(
                    f"VALIDATION FAILED for {table.table_name}: "
                    f"inserted={total_inserted[0]} — delete skipped."
                )
            )
            # return # skip delete and exit

        delete_threads = []
        for shard_idx in range(select_session):
            dt = threading.Thread(
                target=delete_bulk,
                args=(delete_qry, src_conn_str, temp_table_name, table,
                      shard_idx, select_session),
                name=f"delete-{shard_idx}",
                daemon=True,
            )
            delete_threads.append(dt)
            dt.start()

        for dt in delete_threads:
            dt.join()
        print("[main] All deletes done.")

        AuditLog.objects.create(
            user=user,
            action='Run T24 Table',
            details=(
                f"Successfully ran archival for table {table.table_name} "
                f"in application {application.name}"
            )
        )

    except Exception as e:
        logger.error(f"Error running T24 archival for table {table_id}: {e}")

    finally:
        if temp_table_name and src_conn_str:
            try:
                cleanup_conn = pyodbc.connect(src_conn_str)
                cleanup_conn.cursor().execute(
                    f"DROP TABLE IF EXISTS {temp_table_name}"
                )
                cleanup_conn.commit()
                cleanup_conn.close()
                print(f"[main] Temp table {temp_table_name} dropped.")
            except Exception as ce:
                logger.error(f"Failed to drop temp table {temp_table_name}: {ce}")
import logging
from django.db import connection
from .models import ArchivalTable,ArchivalModule
from .utils import get_connection

logger = logging.getLogger(__name__)

def archive_module(module_id, archival_date):
    module = ArchivalModule.objects.get(id=module_id)
    tables = module.tables.all().order_by('sequence')
    results = []
    total_success = True

    for table in tables:
        res = archive_table_batch(table, archival_date)
        results.append({
            'table_id': table.id,
            'table_name': table.table_name,
            'status': res['status'],
            'rows_archived': res.get('rows_archived', 0),
            'error': res.get('error')
        })
        if res['status'] != 'success':
            total_success = False
            break  
    return {'status': 'success' if total_success else 'partial', 'results': results}

def archive_table_batch(table, archival_date):
    app = table.module.application
    if not app.src_conn or not app.dstn_conn:
        return {'status': 'error', 'error': 'Missing source/destination connection'}

    src_conn = get_connection(app.src_conn.name)
    dst_conn = get_connection(app.dstn_conn.name)

    
    try:    
        select_sql = table.select_script.format(archival_date=archival_date)
        with src_conn.cursor() as cursor:
            cursor.execute(select_sql)
            ids = [row[0] for row in cursor.fetchall()]
        if not ids:
            return {'status': 'success', 'rows_archived': 0}

        
        insert_sql = table.insert_script
        delete_sql = table.delete_script
        if '{archival_date}' in insert_sql:
            insert_sql = insert_sql.format(archival_date=archival_date)
        if '{archival_date}' in delete_sql:
            delete_sql = delete_sql.format(archival_date=archival_date)

        identity_enabled = False
        if table.identity_insert:            
            try:
                with dst_conn.cursor() as cur:
                    cur.execute(f"SET IDENTITY_INSERT {table.table_name} ON")
                    dst_conn.commit()
                identity_enabled = True
            except Exception as e:
                return {'status': 'error', 'error': f'Failed to enable IDENTITY_INSERT: {str(e)}'}
        print(table.table_name, identity_enabled)
        batch_size = app.volume
        total_inserted = 0
        with dst_conn.cursor() as dst_cursor:
            for i in range(0, len(ids), batch_size):
                chunk = ids[i:i+batch_size]
                
                placeholders = ','.join(['?' for _ in chunk])
                final_insert = insert_sql.replace('{ids}', placeholders)
                print("Query:", final_insert)
                print("Placeholders count:", final_insert.count('?'))
                print("Params count:", len(chunk))
                # print(final_insert)
                dst_cursor.execute(final_insert, chunk)
                dst_conn.commit()
                total_inserted += dst_cursor.rowcount

        print(f"Archived {total_inserted} rows for table {table.table_name}")

        if table.acct_sum == 'Y':
            create_temp = f"""
            IF OBJECT_ID('{table.table_name}_TEMP', 'U') IS NOT NULL
                DROP TABLE {table.table_name}_TEMP;

            CREATE TABLE {table.table_name}_TEMP (
                    ACCT_NO VARCHAR(50),
                    SUB_OPRN_TYPE VARCHAR(5),
                    TOTAL_AMNT DECIMAL(18,2),
                    TOTAL_AMNT_BASE DECIMAL(18,2),
                    TOTAL_AMNT_FRGN DECIMAL(18,2)
                );
                """        
            insert_temp = """MERGE INTO {table_name}_TEMP AS TARGET
                USING (
                    SELECT CASE WHEN SUB_OPRN_TYPE='00001' THEN DEBT_ACCT ELSE CRDT_ACCT END AS ACCT_NO,
                    SUB_OPRN_TYPE,
                    SUM(AMNT) AS TOTAL_AMNT,
                    SUM(AMNT_BASE_CRCY) AS TOTAL_AMNT_BASE,
                    SUM(AMNT_FRGN_CRCY) AS TOTAL_AMNT_FRGN
                    FROM {table_name}
                    WHERE ACCT_TRAN_ID IN ({ids})
                    GROUP BY CASE WHEN SUB_OPRN_TYPE='00001' THEN DEBT_ACCT ELSE CRDT_ACCT END, SUB_OPRN_TYPE
                ) AS SOURCE
                ON TARGET.ACCT_NO = SOURCE.ACCT_NO AND TARGET.SUB_OPRN_TYPE = SOURCE.SUB_OPRN_TYPE
                WHEN MATCHED THEN
                    UPDATE SET
                        TOTAL_AMNT = TARGET.TOTAL_AMNT + SOURCE.TOTAL_AMNT,
                        TOTAL_AMNT_BASE = TARGET.TOTAL_AMNT_BASE + SOURCE.TOTAL_AMNT_BASE,
                        TOTAL_AMNT_FRGN = TARGET.TOTAL_AMNT_FRGN + SOURCE.TOTAL_AMNT_FRGN
                WHEN NOT MATCHED THEN
                    INSERT (ACCT_NO, SUB_OPRN_TYPE, TOTAL_AMNT, TOTAL_AMNT_BASE, TOTAL_AMNT_FRGN)
                    VALUES (SOURCE.ACCT_NO, SOURCE.SUB_OPRN_TYPE, SOURCE.TOTAL_AMNT, SOURCE.TOTAL_AMNT_BASE, SOURCE.TOTAL_AMNT_FRGN)
                """
            insert_temp = insert_temp.format(table_name=table.table_name)
            

            with src_conn.cursor() as src_cursor:
                src_cursor.execute(create_temp)
                src_conn.commit()
            
            with src_conn.cursor() as src_cursor:
                for i in range(0, len(ids), batch_size):
                    chunk = ids[i:i+batch_size]
                    placeholders = ','.join(['?' for _ in chunk])
                    final_insert = insert_temp.replace('{ids}', placeholders)
                    src_cursor.execute(final_insert, chunk)
                src_conn.commit()
        

        if identity_enabled:
            try:
                with dst_conn.cursor() as cur:
                    cur.execute(f"SET IDENTITY_INSERT {table.table_name} OFF")
                    dst_conn.commit()
            except:
                pass        
        # total_deleted = 0
        # with src_conn.cursor() as src_cursor:
        #     for i in range(0, len(ids), batch_size):
        #         chunk = ids[i:i+batch_size]
        #         placeholders = ','.join(['?' for _ in chunk])
        #         final_delete = delete_sql.replace('{ids}', placeholders)
        #         src_cursor.execute(final_delete, chunk)
        #         src_conn.commit()
        #         total_deleted += src_cursor.rowcount

        return {'status': 'success', 'rows_archived': total_inserted}
    except Exception as e:
        logger.exception(f"Error archiving table {table.table_name}")
        return {'status': 'error', 'error': str(e)}
    finally:
        src_conn.close()
        dst_conn.close()
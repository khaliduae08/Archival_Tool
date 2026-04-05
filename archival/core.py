import logging
import uuid
from django.db import connection
from .models import ArchivalTable,ArchivalModule, TempArchivalIds
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
    temp_table_name = f"#IDS_{table.table_name}_{uuid.uuid4().hex}"
    app = table.module.application
    if not app.src_conn or not app.dstn_conn:
        return {'status': 'error', 'error': 'Missing source/destination connection'}

    src_conn = get_connection(app.src_conn.name)
    dst_conn = get_connection(app.dstn_conn.name)

    
    try:    
        select_sql = table.select_script.format(archival_date=archival_date)
        with src_conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE {temp_table_name} (
                RECID BIGINT PRIMARY KEY NONCLUSTERED);

                CREATE CLUSTERED INDEX IX_{temp_table_name}_RECID 
                    ON {temp_table_name}(RECID);
                    """)
            src_conn.commit()
        
        with src_conn.cursor() as cur:
            insert_temp_sql = f"""
                INSERT INTO {temp_table_name} (RECID)
                {select_sql}
            """
            cur.execute(insert_temp_sql)
            src_conn.commit()

        
        insert_sql = table.insert_script
        delete_sql = table.delete_script            

        if table.delete_script:
            if '{archival_date}' in delete_sql:
                delete_sql = delete_sql.format(archival_date=archival_date)

        if '{archival_date}' in insert_sql:
            insert_sql = insert_sql.format(archival_date=archival_date)

        final_insert = insert_sql.replace(
            "IN ({ids})",
            f"IN (SELECT RECID FROM {temp_table_name})"
        )
        if delete_sql:
            final_delete = delete_sql.replace(
                "IN ({ids})",
                f"IN (SELECT RECID FROM {temp_table_name})"
            )

        with src_conn.cursor() as cur:
            cur.execute(final_insert)
            src_conn.commit()
        
        if table.acct_sum == 'Y':
            temp_acct_tran=f"{table.table_name}_{uuid.uuid4().hex}"
            app_name=table.module.application.name
            get_tran_type_code=f"""SELECT TRAN_ID FROM [TRAN] WHERE TRAN_NAME ='{app_name}_ARCHIVAL'"""
            insert_tran_type_code=f"""INSERT INTO [TRAN] (TRAN_NAME,TRAN_NAME_LOCAL,TRAN_STTS,SRVC_TYPE_CODE,SUB_SRVC_TYPE_CODE,TRAN_DESC,IS_DELETED  
                                        ,IS_ACTIVE,SESSION_ID,SESSION_CODE,CREATED_BY,CREATED_ON)  
                                        SELECT TOP 1 '{app_name}_ARCHIVAL','{app_name}_ARCHIVAL','00002','00003','00004','{app_name} ARCHIVAL',0,1,'574805','{app_name}ARCHIVALSESSION',47,getdate()  
                                        where not exists (SELECT 1 FROM [TRAN] WHERE TRAN_NAME='{app_name}_ARCHIVAL')"""
            # print(insert_tran_type_code)
            with src_conn.cursor() as cur:
                cur.execute(insert_tran_type_code)                               
                tran_type_code = cur.execute(get_tran_type_code).fetchone()[0]
                src_conn.commit()


            create_agg_table = f"""
                IF OBJECT_ID('{temp_acct_tran}', 'U') IS NOT NULL
                    DROP TABLE {temp_acct_tran};
                CREATE TABLE {temp_acct_tran} (
                    ACCT_NO VARCHAR(50),
                    SUB_OPRN_TYPE VARCHAR(5),
                    TOTAL_AMNT DECIMAL(18,2),
                    TOTAL_AMNT_BASE DECIMAL(18,2),
                    TOTAL_AMNT_FRGN DECIMAL(18,2)
                );
            """
            agg_insert_sql = f"""
                INSERT INTO {temp_acct_tran}
                SELECT 
                    CASE 
                        WHEN SUB_OPRN_TYPE = '00001' THEN DEBT_ACCT 
                        ELSE CRDT_ACCT 
                    END AS ACCT_NO,
                    SUB_OPRN_TYPE,
                    SUM(AMNT),
                    SUM(AMNT_BASE_CRCY),
                    SUM(AMNT_FRGN_CRCY)
                FROM {table.table_name} A
                JOIN {temp_table_name} I ON A.ACCT_TRAN_ID = I.RECID
                GROUP BY 
                    CASE 
                        WHEN SUB_OPRN_TYPE = '00001' THEN DEBT_ACCT 
                        ELSE CRDT_ACCT 
                    END,
                    SUB_OPRN_TYPE
            """
            with src_conn.cursor() as cur:
                cur.execute(create_agg_table)
                cur.execute(agg_insert_sql)
                src_conn.commit()        

            if final_delete:
                with src_conn.cursor() as cur:
                            cur.execute(final_delete)
                            src_conn.commit()

            agg_acct_tran=f"""MERGE ACCT_TRAN AS TARGET USING (SELECT    
                TOTAL_AMNT AS AMNT, TOTAL_AMNT_BASE AS AMNT_BASE_CRCY,  TOTAL_AMNT_FRGN AS AMNT_FRGN_CRCY,'00000' AS AMNT_TYPE_CODE,TOTAL_AMNT_BASE/replace(isnull(TOTAL_AMNT,1),0,1) AS XCHG_RATE,    
                CASE WHEN A.SUB_OPRN_TYPE='00002' then ACCT_NO else '00009999' end AS DEBT_ACCT,  
                CASE WHEN A.SUB_OPRN_TYPE='00001' then ACCT_NO else '00009999' end AS CRDT_ACCT,  
                ISNULL(C.BP_MAIN_ID,9999) AS BSNS_PRTN_ID, '00000' AS BP_TYPE,'00014' AS OPRN_TYPE, A.SUB_OPRN_TYPE AS SUB_OPRN_TYPE,  
                cast(getdate() as date) AS TRAN_DATE, {tran_type_code} AS TRAN_TYPE_CODE, C.ACCT_ID  AS TRAN_TYPE_ID,ISNULL(C.CURR_ID,'00003') CRCY_CODE,   
                '00002' AS SRCE_TYPE_CODE, '00004' AS TRGT_TYPE_CODE, 47 AS SESSION_ID, 'DATA_ARCH' AS SESSION_CODE, 77 AS CREATED_BY,  
                GETDATE() AS CREATED_ON, NULL AS UPDATED_BY,NULL AS UPDATED_ON,NULL AS IS_POSTED_TO_ACCT, cast(getdate() as date) AS BSNS_OPRN_DATE ,
                1 AS IS_ACTIVE,0 IS_DELETED, NULL Reference_Number
                FROM {temp_acct_tran} A  
                LEFT OUTER JOIN DBO.ACCT C ON A.ACCT_NO=C.WALT_ACCT_NMBR COLLATE SQL_LATIN1_GENERAL_CP1256_CI_AS  
                )AS SOURCE   
                ON SOURCE.CRDT_ACCT = TARGET.CRDT_ACCT AND SOURCE.DEBT_ACCT = TARGET.DEBT_ACCT   
                AND SOURCE.SUB_OPRN_TYPE=TARGET.SUB_OPRN_TYPE AND TARGET.TRAN_TYPE_ID =SOURCE.TRAN_TYPE_ID
                AND TARGET.TRAN_TYPE_CODE=SOURCE.TRAN_TYPE_CODE
                WHEN NOT MATCHED BY TARGET THEN INSERT (  
                AMNT,AMNT_BASE_CRCY,AMNT_FRGN_CRCY,AMNT_TYPE_CODE,XCHG_RATE,DEBT_ACCT,CRDT_ACCT,BSNS_PRTN_ID, BP_TYPE, OPRN_TYPE, SUB_OPRN_TYPE,  
                TRAN_DATE, TRAN_TYPE_CODE, TRAN_TYPE_ID, CRCY_CODE, SRCE_TYPE_CODE, TRGT_TYPE_CODE, 
                SESSION_ID, SESSION_CODE, CREATED_BY, CREATED_ON, UPDATED_BY, UPDATED_ON, IS_POSTED_TO_ACCT,BSNS_OPRN_DATE,IS_ACTIVE,IS_DELETED,Reference_Number) VALUES(  
                SOURCE.AMNT, SOURCE.AMNT_BASE_CRCY, SOURCE.AMNT_FRGN_CRCY, SOURCE.AMNT_TYPE_CODE, SOURCE.XCHG_RATE, SOURCE.DEBT_ACCT, SOURCE.CRDT_ACCT,  
                SOURCE.BSNS_PRTN_ID, SOURCE.BP_TYPE,SOURCE.OPRN_TYPE, SOURCE.SUB_OPRN_TYPE, SOURCE.TRAN_DATE, SOURCE.TRAN_TYPE_CODE,  
                SOURCE.TRAN_TYPE_ID, SOURCE.CRCY_CODE,SOURCE.SRCE_TYPE_CODE,SOURCE.TRGT_TYPE_CODE,SOURCE.SESSION_ID,SOURCE.SESSION_CODE, 
                SOURCE.CREATED_BY,SOURCE.CREATED_ON,SOURCE.UPDATED_BY,SOURCE.UPDATED_ON,SOURCE.IS_POSTED_TO_ACCT, SOURCE.BSNS_OPRN_DATE,
                SOURCE.IS_ACTIVE,SOURCE.IS_DELETED, SOURCE.Reference_Number)  
                WHEN MATCHED THEN   
                UPDATE SET TARGET.AMNT= TARGET.AMNT+SOURCE.AMNT,TARGET.AMNT_BASE_CRCY= TARGET.AMNT_BASE_CRCY+SOURCE.AMNT_BASE_CRCY,TARGET.AMNT_FRGN_CRCY= TARGET.AMNT_FRGN_CRCY+SOURCE.AMNT_FRGN_CRCY,TARGET.UPDATED_ON= GETDATE(),TARGET.SESSION_CODE= 'DATA ARCHIVAL' ;
                """
            with src_conn.cursor() as cur:
                cur.execute(agg_acct_tran)
                src_conn.commit()

        if delete_sql:
            final_delete = delete_sql.replace(
                "IN ({ids})",
                f"IN (SELECT RECID FROM {temp_table_name})"
                )

            with src_conn.cursor() as src_cursor:
                src_cursor.execute(final_delete)
                src_conn.commit() 

        with src_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
            rows_archived = cur.fetchone()[0]

        return {'status': 'success', 'rows_archived': rows_archived}

    except Exception as e:
        logger.exception(f"Error archiving table {table.table_name}")
        return {'status': 'error', 'error': str(e)}

    finally:
        # Clean up temp table
        try:
            with src_conn.cursor() as cur:
                cur.execute(f"DROP TABLE {temp_table_name}")
                cur.execute(f"DROP TABLE IF EXISTS {temp_acct_tran}")
                src_conn.commit()
        except Exception:
            pass
        src_conn.close()
        dst_conn.close()

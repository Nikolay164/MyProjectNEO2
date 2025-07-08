from datetime import datetime
from airflow.exceptions import AirflowException
from helpers.etl_utils import get_db_connection

def log_to_db(process_name, status, rows_processed=0, error_message=None):#Логирование результатов выполнения в БД
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO logs.etl_logs 
                    (process_name, start_time, end_time, status, rows_processed, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    process_name,
                    datetime.now(),
                    datetime.now(),
                    status,
                    rows_processed,
                    error_message
                ))
                conn.commit()
    except Exception as e:
        raise AirflowException(f"Ошибка при логировании: {e}")

def _execute_procedure(proc_name, param=None):#функция выполнения процедур с логированием
    start_time = datetime.now()
    log_id = None
    rows_affected = 0
    
    try:
        # Логируем начало выполнения
        log_to_db(proc_name, 'STARTED')
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                if param:
                    cursor.execute(f"CALL {proc_name}(%s)", (param,))
                else:
                    cursor.execute(f"CALL {proc_name}()")
                rows_affected = cursor.rowcount
                conn.commit()
        
        # Логируем успех
        log_to_db(proc_name, "SUCCESS")   
        
        return rows_affected
        
    except Exception as e:
        # Логируем ошибку
        log_to_db(proc_name, "ERROR")   
        raise AirflowException(f"Ошибка выполнения {proc_name}: {str(e)}")

def init_balances(): #Инициализация остатков на 31.12.2017
    return _execute_procedure("ds.init_account_balance_20171231")

def calculate_dm_tables_for_january_2018(): #функция расчета витрин за январь 2018
    for day in range(1, 32):
        current_date = datetime(2018, 1, day).date()
        try:
            _execute_procedure("ds.fill_account_turnover_f", current_date)
            _execute_procedure("ds.fill_account_balance_f", current_date)
        except Exception as e:
            print(f"Пропуск даты {current_date} из-за ошибки: {str(e)}")
            continue
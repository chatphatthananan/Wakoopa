import config
from SGTAMProdTask import SGTAMProd

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

import xlsxwriter
import os
import logging


# Calculate yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')  # Format it in Y-m-d which is common for SQL

# Calculate today's date
today_date = datetime.now()
today_str = today_date.strftime('%Y-%m-%d')  # Format it in Y-m-d which is common for SQL

# Database connection parameters
driver = '{ODBC Driver 17 for SQL Server}'
server = 'xxx'
database = 'xxx'
username = 'xxx'
password = 'xxx'

# Create connection string for pyodbc
conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

# Create engine using SQLAlchemy
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

# SQL queries
query1_daily_migrated_users = f"SELECT * FROM tWakoopaMember WHERE CAST(created_at AS DATE) = '{yesterday_str}' AND panel_person_id = 1 AND ReferenceDate = '{today_str}' ORDER BY created_at, panel_household_id"
query2_migrated_master_list = f"SELECT * FROM tWakoopaMember WHERE CAST(created_at AS DATE) >= '2023-11-08' ORDER BY ReferenceDate, created_at, panel_household_id"
query3_migrated_master_list_today = f"SELECT * FROM tWakoopaMember WHERE CAST(created_at AS DATE) >= '2023-11-08' AND ReferenceDate = '{today_str}' AND panel_person_id = 1 ORDER BY created_at, panel_household_id"
query4_deviceidcountmorethanone = f"SELECT panel_household_id,device_id,MIN(ReferenceDate) AS earliest_ReferenceDate,COUNT(DISTINCT device_type) AS distinct_device_type,COUNT(DISTINCT model) AS distinct_model FROM [SGTAMProd].[dbo].[tWakoopaDevice] WHERE device_type IN ('SMARTPHONE', 'TABLET') GROUP BY panel_household_id,device_id HAVING COUNT(DISTINCT model) > 1 ORDER BY MIN(ReferenceDate),panel_household_id,device_id"
query5_deviceidfulllist = f"SELECT panel_household_id, device_id,device_type,manufacturer,model,MIN(ReferenceDate) AS earliest_ReferenceDate FROM [SGTAMProd].[dbo].[tWakoopaDevice] WHERE device_type IN ('SMARTPHONE', 'TABLET') GROUP BY panel_household_id,device_id,device_type,manufacturer,model ORDER BY panel_household_id,device_id,device_type,manufacturer,model"

# Excel file path
formatted_date = today_date.strftime("%Y%m%d")
excel_file_path = f'D:\\SGTAM_DP\\Working Project\\Wakoopa\\source\\history\\{formatted_date}\\Wakoopa_Migrated_Users.xlsx'

# Use the engine in a try-except block
try:

    # Set up logging
    log_filename = f"D:/SGTAM_DP/Working Project/Wakoopa/source/log/checkWakoopaMigratedUsers_{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.txt"
    logging.basicConfig(filename=log_filename, level=logging.INFO)
    s = SGTAMProd()

    config.SGTAM_log_config['statusFlag'], config.SGTAM_log_config['logID']  = s.insert_tlog(**config.SGTAM_log_config)

    # Read data from SQL using pandas
    df_daily_new_list = pd.read_sql(query1_daily_migrated_users, engine)
    df_master_list = pd.read_sql(query2_migrated_master_list, engine)
    df_master_list_today = pd.read_sql(query3_migrated_master_list_today, engine)
    df_deviceidcountmorethanone = pd.read_sql(query4_deviceidcountmorethanone, engine)
    df_deviceidfulllist = pd.read_sql(query5_deviceidfulllist, engine)

    if not os.path.exists(os.path.dirname(excel_file_path)):
        os.makedirs(os.path.dirname(excel_file_path))

    # Write the DataFrames to an Excel file with two sheets
    with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
        df_daily_new_list.to_excel(writer, sheet_name='New Migrated Users', index=False)
        df_master_list.to_excel(writer, sheet_name='Master List', index=False)
        df_master_list_today.to_excel(writer, sheet_name='Master List Today Only', index=False)
        df_deviceidcountmorethanone.to_excel(writer, sheet_name='Device_ID_CountMoreThanOne', index=False)
        df_deviceidfulllist.to_excel(writer, sheet_name='Device_ID_FullList', index=False)
        # Auto-adjust columns' width
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(df_daily_new_list.columns if sheet_name == 'New Migrated Users' else df_master_list.columns):
                column_len = max(df_daily_new_list[col].astype(str).str.len().max() if sheet_name == 'New Migrated Users' else df_master_list[col].astype(str).str.len().max(), len(col))
                worksheet.set_column(i, i, column_len + 1)  # Added 1 for a little extra margin

    if df_daily_new_list.empty:
        print(f'No new panelists migrated yesterday {yesterday_str}')
        logging.info(f'No new panelists migrated yesterday {yesterday_str}')
        config.SGTAM_log_config['logMsg'] = f'No new panelists migrated yesterday {yesterday_str}'
        config.email['body'] = f"No new panelists migrated yesterday {yesterday_str}. \n*This is an auto generated email, do not reply to this email."
    else:
        print(f'There are new panelists migrated yesterday {yesterday_str}')
        logging.info(f'There are new panelists migrated yesterday {yesterday_str}')
        config.SGTAM_log_config['logMsg'] = f'There are new panelists migrated yesterday {yesterday_str}'
        config.email['body'] = f"There are new panelists migrated yesterday {yesterday_str}. \n*This is an auto generated email, do not reply to this email."


    
    config.email['to'] = 'xxx'
    config.email['subject'] = f"Daily Check Wakoopa Migrated Users - {today_date}"
    config.email['filename'] = f"{excel_file_path}"
    
    s.send_email(**config.email)
    logging.info('Email sent.')
    s.update_tlog(**config.SGTAM_log_config)
    logging.info('SGTAM log updated.')    

except Exception as e:
    print(f"An error occurred: {e}")
    logging.info(f"An error occurred: {e}")
    config.SGTAM_log_config['logMsg'] = f"An error occurred:\n{e}"
    config.SGTAM_log_config['statusFlag'] = 2
    config.email['to'] = 'xxx'
    config.email['subject'] = f"[ERROR] Daily Check Wakoopa Migrated Users - {today_date}"
    config.email['body'] = f"An error occurred: \n{e} \n*This is an auto generated email, do not reply to this email."
    config.email['filename'] = f"{log_filename}"

    s.send_email(**config.email)
    logging.info('Email sent.')
    s.update_tlog(**config.SGTAM_log_config)
    logging.info('SGTAM log updated.')    

finally:
    print('Enter finally clause.')
    logging.info('Enter finally clause.')
    # Dispose the engine
    engine.dispose()
    print('Ensure SQL Alchemy engine is stopped and disposed.')
    logging.info('Ensure SQL Alchemy engine is stopped and disposed.')
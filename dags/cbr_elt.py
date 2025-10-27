from datetime import datetime, timedelta
import json
import requests
import xml.etree.ElementTree as ET
import zipfile
import io
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
# from airflow.utils.dates import days_ago
from pendulum import datetime, yesterday


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': yesterday('UTC'),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def get_current_date_string():
    """Возвращает текущую дату в формате YYYYMMDD для URL архива банков....."""
    return datetime.now().strftime("%Y%m%d")

def get_currency_rates(**kwargs):
    # Получаем параметры из Variables
    cbr_url = Variable.get("CBR_CURRENCY_URL", 
                         default_var="https://www.cbr.ru/scripts/XML_daily.asp")
    db_params = {
        'host': Variable.get("DB_HOST", default_var="postgres"),
        'port': Variable.get("DB_PORT", default_var="5432"),
        'database': Variable.get("DB_NAME", default_var="airflow"),
        'user': Variable.get("DB_USER", default_var="airflow"),
        'password': Variable.get("DB_PASSWORD", default_var="airflow")
    }
    
    # Получаем данные с сайта ЦБ РФ
    response = requests.get(cbr_url)
    response.raise_for_status()
    xml_data = response.text
    
    # Парсим XML
    root = ET.fromstring(xml_data)
    date_str = root.attrib['Date']
    conversion_date = datetime.strptime(date_str, "%d.%m.%Y").date()
    
    # Подключаемся к БД
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # Создаем таблицу, если она не существует
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS currency_rates (
            id SERIAL PRIMARY KEY,
            from_currency VARCHAR(3) NOT NULL,
            to_currency VARCHAR(3) NOT NULL DEFAULT 'RUB',
            conversion_date DATE NOT NULL,
            conversion_type VARCHAR(20) DEFAULT 'DAILY',
            conversion_rate NUMERIC(15, 6) NOT NULL,
            status_code VARCHAR(10) DEFAULT 'ACTIVE',
            creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(50) DEFAULT 'AIRFLOW',
            last_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_update_by VARCHAR(50) DEFAULT 'AIRFLOW',
            last_update_login VARCHAR(50),
            UNIQUE (from_currency, to_currency, conversion_date)
        )
    """)
    
    # Помечаем старые записи для этой даты как неактивные
    cursor.execute("""
        UPDATE currency_rates 
        SET status_code = 'INACTIVE',
            last_update_date = CURRENT_TIMESTAMP,
            last_update_by = 'AIRFLOW'
        WHERE conversion_date = %s
    """, (conversion_date,))
    
    # Вставляем новые данные
    for valute in root.findall('Valute'):
        from_currency = valute.find('CharCode').text
        value = valute.find('Value').text.replace(',', '.')
        nominal = int(valute.find('Nominal').text)
        conversion_rate = float(value) / nominal
        
        cursor.execute("""
            INSERT INTO currency_rates (
                from_currency, to_currency, conversion_date, 
                conversion_rate, status_code
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (from_currency, to_currency, conversion_date) 
            DO UPDATE SET
                conversion_rate = EXCLUDED.conversion_rate,
                status_code = EXCLUDED.status_code,
                last_update_date = CURRENT_TIMESTAMP
        """, (from_currency, 'RUB', conversion_date, conversion_rate, 'ACTIVE'))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Successfully loaded currency rates for {conversion_date}")

def get_bank_data(**kwargs):
    # Получаем параметры из Variables
    base_url = Variable.get("CBR_BANKS_BASE_URL", 
                          default_var="https://www.cbr.ru/vfs/mcirabis/BIKNew/")
    archive_name = f"{get_current_date_string()}ED01OSBR.zip"
    cbr_url = base_url + archive_name
    
    db_params = {
        'host': Variable.get("DB_HOST", default_var="postgres"),
        'port': Variable.get("DB_PORT", default_var="5432"),
        'database': Variable.get("DB_NAME", default_var="airflow"),
        'user': Variable.get("DB_USER", default_var="airflow"),
        'password': Variable.get("DB_PASSWORD", default_var="airflow")
    }
    
    try:
        # Получаем ZIP архив
        response = requests.get(cbr_url)
        response.raise_for_status()
        zip_data = io.BytesIO(response.content)
        
        # Распаковываем ZIP
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            xml_files = [f for f in zip_ref.namelist() if f.endswith('.xml')]
            if not xml_files:
                raise ValueError("No XML files found in the ZIP archive")
            
            # Берем первый XML файл
            xml_content = zip_ref.read(xml_files[0]).decode('windows-1251')
        
        # Парсим XML с учетом namespace
        ns = {'ns': 'urn:cbr-ru:ed:v2.0'}
        root = ET.fromstring(xml_content)

        # Получаем метаданные из корневого элемента
        ed_date = root.attrib.get('EDDate', '')
        cbrffile = archive_name
        import_date = datetime.now().isoformat()
        
        # Подключаемся к БД
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Создаем таблицу, если она не существует
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS banks (
                id SERIAL PRIMARY KEY,
                bic VARCHAR(9) NOT NULL,
                pzn VARCHAR(10),
                rgn VARCHAR(10),
                ind VARCHAR(10),
                tnp VARCHAR(10),
                nnp VARCHAR(100),
                adr VARCHAR(500),
                namep VARCHAR(500),
                newnum VARCHAR(20),
                regn VARCHAR(20),
                ksnp TEXT,
                datein VARCHAR(20),
                cbrfdate VARCHAR(20) NOT NULL,
                cbrffile VARCHAR(100) NOT NULL,
                crc7 VARCHAR(20),
                import_date TIMESTAMP NOT NULL,
                json JSONB NOT NULL
            )
        """)
        
        # Очищаем таблицу перед загрузкой новых данных
        cursor.execute("TRUNCATE TABLE banks")
        
        # Обрабатываем каждую запись в XML
        for entry in root.findall('ns:BICDirectoryEntry', ns):
            bic = entry.attrib.get('BIC', '')
            participant_info = entry.find('ns:ParticipantInfo', ns)
            
            if participant_info is not None:
                accounts = []
                for account in entry.findall('ns:Accounts', ns):
                    accounts.append({
                        'Account': account.attrib.get('Account', ''),
                        'RegulationAccountType': account.attrib.get('RegulationAccountType', ''),
                        'CK': account.attrib.get('CK', ''),
                        'AccountCBRBIC': account.attrib.get('AccountCBRBIC', ''),
                        'DateIn': account.attrib.get('DateIn', ''),
                        'AccountStatus': account.attrib.get('AccountStatus', '')
                    })
                
                # Собираем SWBICS (если есть)
                swbics = []
                for swbic in entry.findall('ns:SWBICS', ns):
                    swbics.append({
                        'SWBIC': swbic.attrib.get('SWBIC', ''),
                        'DefaultSWBIC': swbic.attrib.get('DefaultSWBIC', '')
                    })
                
                # Формируем JSON со всеми данными банка
                bank_json = {
                    'BIC': bic,
                    'ParticipantInfo': {
                        'NameP': participant_info.attrib.get('NameP', ''),
                        'EnglName': participant_info.attrib.get('EnglName', ''),
                        'RegN': participant_info.attrib.get('RegN', ''),
                        'CntrCd': participant_info.attrib.get('CntrCd', ''),
                        'Rgn': participant_info.attrib.get('Rgn', ''),
                        'Ind': participant_info.attrib.get('Ind', ''),
                        'Tnp': participant_info.attrib.get('Tnp', ''),
                        'Nnp': participant_info.attrib.get('Nnp', ''),
                        'Adr': participant_info.attrib.get('Adr', ''),
                        'DateIn': participant_info.attrib.get('DateIn', ''),
                        'PtType': participant_info.attrib.get('PtType', ''),
                        'Srvcs': participant_info.attrib.get('Srvcs', ''),
                        'XchType': participant_info.attrib.get('XchType', ''),
                        'UID': participant_info.attrib.get('UID', ''),
                        'ParticipantStatus': participant_info.attrib.get('ParticipantStatus', '')
                    },
                    'Accounts': accounts,
                    'SWBICS': swbics
                }
                
                # Объединяем счета через запятую для поля ksnp
                ksnp_value = ','.join([acc['Account'] for acc in accounts if acc['Account']])
                
                data = {
                    'pzn': participant_info.attrib.get('PtType', ''),
                    'rgn': participant_info.attrib.get('Rgn', ''),
                    'ind': participant_info.attrib.get('Ind', ''),
                    'tnp': participant_info.attrib.get('Tnp', ''),
                    'nnp': participant_info.attrib.get('Nnp', ''),
                    'adr': participant_info.attrib.get('Adr', ''),
                    'namep': participant_info.attrib.get('NameP', ''),
                    'newnum': participant_info.attrib.get('PrntBIC', ''),
                    'regn': participant_info.attrib.get('RegN', ''),
                    'ksnp': ksnp_value,
                    'datein': participant_info.attrib.get('DateIn', ''),
                    'cbrfdate': ed_date,
                    'cbrffile': cbrffile,
                    'crc7': bic[-7:] if bic else '',
                    'bic': bic,
                    'import_date': import_date,
                    'bank_json': json.dumps(bank_json, ensure_ascii=False)
                }
                
                # Вставляем данные в таблицу
                cursor.execute("""
                    INSERT INTO banks (
                        pzn, rgn, ind, tnp, nnp, adr, namep, newnum, 
                        regn, ksnp, datein, cbrfdate, cbrffile, crc7, bic,
                        import_date, json
                    )
                    VALUES (
                        %(pzn)s, %(rgn)s, %(ind)s, %(tnp)s, %(nnp)s, %(adr)s, 
                        %(namep)s, %(newnum)s, %(regn)s, %(ksnp)s, %(datein)s, 
                        %(cbrfdate)s, %(cbrffile)s, %(crc7)s, %(bic)s,
                        %(import_date)s, %(bank_json)s::jsonb
                    )
                """, data)
        
        conn.commit()
        print(f"Successfully loaded bank data from {cbrffile} with date {ed_date}")
        
    except Exception as e:
        print(f"Error loading bank data: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Создаем DAG для курсов валют
dag_currency = DAG(
    'cbr_currency_rates',
    default_args=default_args,
    description='Получение курсов валют от ЦБ РФ',
    schedule='0 11 * * *',  # Ежедневно в 11:00 UTC
    tags=['ЦБР', 'rates', 'daily'],
    catchup=False,
)

currency_task = PythonOperator(
    task_id='get_currency_rates',
    python_callable=get_currency_rates,
    dag=dag_currency,
)

# Создаем DAG для данных банков
dag_banks = DAG(
    'cbr_bank_data',
    default_args=default_args,
    description='Получение данных банков от ЦБ РФ',
    schedule='30 11 * * *',  # Ежедневно в 11:30 UTC
    tags=['ЦБР', 'banks', 'daily'],
    catchup=False,
)

bank_task = PythonOperator(
    task_id='get_bank_data',
    python_callable=get_bank_data,
    dag=dag_banks,
)

import pandas as pd
import sqlite3
import json
import csv
import re


def get_csv(path):
    df = pd.read_excel(path, sheet_name='Vehicles', dtype=str)
    path = path.rstrip('xlsx') + 'csv'
    df.to_csv(path, index=False, header=True)
    df = pd.read_csv(path)
    return df, path


def clean_csv(path, dataset):
    path = f'{path.rsplit(".csv")[0]}[CHECKED].csv'
    with open(path, 'w', encoding='utf-8') as new_dataset:
        new_writer = csv.writer(new_dataset, delimiter=',', lineterminator="\n")
        new_writer.writerow(dataset.columns)
        count = len([True for line in list(dataset.values) for val in line if re.sub(r'\D', '', val) != val])
        new_writer.writerows([[re.sub(r'\D', '', val) for val in line] for line in dataset.values])
    df = pd.read_csv(path, delimiter=',')
    return df, count, path


def init_table(df):
    statement = 'CREATE TABLE convoy ('
    for header in df:
        if header == 'vehicle_id':
            statement += f'{header} INTEGER PRIMARY KEY,'
        else:
            statement += f'{header} INTEGER NOT NULL,'
    statement = statement + 'score INTEGER DEFAULT 0 NOT NULL)'
    return statement


def write_db(path, df):
    path = re.sub(r'\[CHECKED].csv\Z', '.s3db', path)
    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        cursor.execute(init_table(df))
        conn.commit()
        df.to_sql('convoy', conn, if_exists='append', index=False)
    except sqlite3.Error as error:
        print(error)
    finally:
        conn.close()
    return path, len(df.values)


def write_score(path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    result = cursor.execute('SELECT * FROM convoy').fetchall()
    for record in result:
        cursor.execute(f"""
        UPDATE convoy 
        SET score={score_get(record[1], record[2], record[3])}
        WHERE vehicle_id={record[0]}
        """)
    conn.commit()
    conn.close()


def fill_json(path, df):
    path = re.sub(r'\.s3db\Z', '.json', path)
    df = df.to_dict(orient='records')
    with open(path, 'w') as file:
        json.dump({'convoy': df}, file)
    return path, len(df)


def fill_xml(path, df):
    xml_path = re.sub(r'\.s3db\Z', '.xml', path)
    if len(df) > 0:
        df.to_xml(xml_path, root_name='convoy', row_name='vehicle', index=False, xml_declaration=False)
    else:
        with open(xml_path, 'w') as f:
            f.write('<convoy></convoy>')
            f.close()
    return xml_path, len(df)


def read_db(path, output_format):
    conn = sqlite3.connect(path)
    if output_format == 'json':
        df = pd.DataFrame(pd.read_sql_query("""SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load
                                                FROM convoy WHERE score > 3""", conn))
    else:
        df = pd.DataFrame(pd.read_sql_query("""SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load
                                                FROM convoy WHERE score <= 3""", conn))
    conn.close()
    return path, df


def pitstop_count(engine_capacity, fuel_consumption):
    route = 450
    burned_fuel = (route / 100) * fuel_consumption
    number_pitstop = burned_fuel / engine_capacity
    return int(number_pitstop), int(burned_fuel)


def score_get(engine_capacity, fuel_consumption, maximum_load):
    score = 0
    number_pitstop, liters_consumed = pitstop_count(engine_capacity, fuel_consumption)
    if number_pitstop >= 2:
        pass
    elif number_pitstop == 1:
        score += 1
    else:
        score += 2
    if liters_consumed <= 230:
        score += 2
    else:
        score += 1
    if maximum_load >= 20:
        score += 2
    else:
        pass
    return score


if __name__ == '__main__':
    file_path = input('Input file_path name\n').strip()
    if file_path.endswith('xlsx'):
        data_frame, file_path = get_csv(file_path)
        print(f'{data_frame.shape[0]} {"lines were" if data_frame.shape[0] > 1 else "line was"} added to {file_path}')
    if file_path.endswith('.csv') and not file_path.endswith('[CHECKED].csv'):
        data_frame = pd.read_csv(file_path, dtype=str)
        data_frame, counter, file_path = clean_csv(file_path, data_frame)
        print(f'{counter} {"cells were" if counter > 1 else "cell was"} corrected in {file_path}')
    if file_path.endswith('[CHECKED].csv'):
        data_frame = pd.read_csv(file_path, dtype=str, delimiter=',')
        file_path, counter = write_db(file_path, data_frame)
        print(f'{counter} {"records were" if data_frame.shape[0] > 1 else "record was"} inserted to {file_path}')
    if file_path.endswith('.s3db'):
        new_path = file_path
        write_score(new_path)
        file_path, data_frame = read_db(new_path, 'json')
        file_path, counter = fill_json(file_path, data_frame)
        print(f'{counter} {"vehicles were" if counter > 1 else "vehicle was"} saved into {file_path}')
        file_path, data_frame = read_db(new_path, 'xml')
        file_path, counter = fill_xml(file_path, data_frame)
        print(f'{counter} {"vehicles were" if counter != 1 else "vehicle was"} saved into {file_path}')
        # some comment
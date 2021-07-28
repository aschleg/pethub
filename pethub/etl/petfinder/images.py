from dotenv import load_dotenv
import json
from urllib.request import urlretrieve
import os
import boto3
import psycopg2
import concurrent.futures


s3_session = boto3.Session(
    aws_access_key_id=os.environ.get('PETHUB_AWS_SECRET_ID'),
    aws_secret_access_key=os.environ.get('PETHUB_AWS_SECRET_ACCESS_KEY')
)
s3_client = s3_session.client("s3")


def download_images(table):
    conn = _create_connection()

    with conn:
        with conn.cursor() as cur:
            if table == 'animals':
                id_column = 'animal_id'
            else:
                id_column = 'org_id'
            cur.execute("""SELECT {id_column}, image_size, url 
                           FROM {table}.photos 
                           WHERE s3_location IS NOT NULL;""".format(id_column=id_column,
                                                                    table=table))
            photos_to_download = [p for p in cur.fetchall()]
    conn.close()

    try:
        os.makedirs('photos')
    except OSError:
        pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(_download_photo, photos_to_download, table)


def _download_photo(photo, table):
    filename = photo[0] + '_' + photo[1]
    save_path = os.path.join('photos', filename)
    urlretrieve(photo[2], save_path)
    s3_location = os.path.join('images', table, save_path)
    s3_client.upload_file(save_path, 'pethub-data', s3_location)

    conn = _create_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""UPDATE {table} 
                           SET s3_location = %s""".format(table=table),
                        s3_location)
    print('filename: ', filename, 'uploaded and inserted.')
    conn.close()


def _create_connection():
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD")
    )

    return conn

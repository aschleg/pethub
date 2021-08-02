import urllib.error

from dotenv import load_dotenv
from urllib.request import urlretrieve
import os
import boto3
import psycopg2
import concurrent.futures


load_dotenv('../../.env')
"""Load the needed environment variables. These could also be system level variables if you are running the script 
on your local machine."""

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
                           WHERE s3_location IS NULL;""".format(id_column=id_column,
                                                                table=table))
            photos_to_download = [p for p in cur.fetchall()]
    conn.close()
    try:
        os.makedirs('photos')
    except OSError:
        pass

    with concurrent.futures.ThreadPoolExecutor(8) as executor:
        future_photo = {executor.submit(_download_photo, photo, table, id_column): photo for photo in photos_to_download}
        for future in concurrent.futures.as_completed(future_photo):
            p = future_photo[future]
            print(p)
            try:
                data = future.result()
            except:
                print(data, p)

    try:
        os.removedirs('photos')
    except OSError:
        pass


def _download_photo(photo, table, id_column):
    filename = photo[0] + '_' + photo[1] + '_' + photo[2].split('/?')[0][-1] + '.jpeg'
    save_path = os.path.join('photos', filename)

    try:
        urlretrieve(photo[2], save_path)
        s3_location = os.path.join('petfinder/images', table, save_path)
        s3_client.upload_file(save_path, 'pethub-data', s3_location)

        conn = _create_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""UPDATE {table}.photos 
                               SET s3_location = %s 
                               WHERE {id_column} = %s 
                                AND image_size = %s 
                                AND url = %s;""".format(table=table, id_column=id_column),
                            ('s3://pethub-data/' + s3_location, photo[0], photo[1], photo[2]))
                conn.commit()

        try:
            os.remove(save_path)
        except OSError:
            pass
        conn.close()
    except urllib.error.HTTPError:
        pass


def _create_connection():
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD")
    )

    return conn


if __name__ == '__main__':
    download_images('animals')

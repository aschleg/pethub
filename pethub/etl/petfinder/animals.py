from dotenv import load_dotenv
import json
import os
import petpy
from petpy import exceptions
import datetime
import boto3
import psycopg2


load_dotenv('../../../.env')

petfinder_access_key = os.environ.get('PETFINDER_PETHUB_KEY')
petfinder_secret_key = os.environ.get('PETFINDER_PETHUB_SECRET_KEY')
pf = petpy.Petfinder(key=petfinder_access_key,
                     secret=petfinder_secret_key)
s3_session = boto3.Session(
    aws_access_key_id=os.environ.get('PETHUB_AWS_SECRET_ID'),
    aws_secret_access_key=os.environ.get('PETHUB_AWS_SECRET_ACCESS_KEY')
)
s3_client = s3_session.client("s3")


def get_animals():
    conn = _create_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT id 
                            FROM organizations.organization;''')
            organization_ids = [f[0] for f in cur.fetchall()]
    for org in organization_ids:
        animals = []
        try:
            animals_at_orgs = pf.animals(organization_id=org, results_per_page=100, pages=None)
            animals_at_orgs = animals_at_orgs['animals']
            for animal in animals_at_orgs:
                animal_id = animal['id']
                ani = dict((k, animal[k]) for k in ['id', 'organization_id', 'url', 'type', 'species', 'age',
                                                    'gender', 'size', 'coat', 'tags', 'name', 'description']
                           if k in animal)

                ani_status = dict((k, animal[k]) for k in ['status', 'status_changed_at', 'published_at']
                                  if k in animal)
                ani_status['animal_id'] = animal_id
                ani_status = json.dumps(ani_status)

                attributes = animal['attributes']
                attributes = json.dumps(attributes)

                breed = animal['breeds']
                breed = json.dumps(breed)

                colors = animal['colors']
                colors = json.dumps(colors)

                environment = animal['environment']
                environment = json.dumps(environment)

                photos = animal['photos']
                videos = animal['videos']

                ani = json.dumps(ani)
                ani = ani.replace("'", "''")

                with conn:
                    with conn.cursor() as cur:
                        cur.execute('''INSERT INTO animals.animal 
                                        SELECT * 
                                        FROM json_populate_record(null::animals.animal, '{animal}')
                                        ON CONFLICT ON CONSTRAINT animal_pk
                                        DO UPDATE SET 
                                            url = EXCLUDED.url,
                                            type = EXCLUDED.type, 
                                            species = EXCLUDED.species, 
                                            age = EXCLUDED.age,
                                            gender = EXCLUDED.gender,
                                            size = EXCLUDED.size,
                                            coat = EXCLUDED.coat,
                                            name = EXCLUDED.name,
                                            description = EXCLUDED.description,
                                            tags = EXCLUDED.tags;'''.format(animal=ani))

                        cur.execute('''INSERT INTO animals.animal_status (animal_id, status, status_changed_at, published_at) 
                                        SELECT animal_id, status, status_changed_at, published_at
                                        FROM json_populate_record(null::animals.animal_status, '{animal_status}');'''
                                    .format(animal_status=ani_status))

                        cur.execute('''INSERT INTO animals.attributes (animal_id, attribute, status) 
                                        SELECT '{animal_id}' as "animal_id", *
                                        FROM json_each_text('{attributes}')
                                        ON CONFLICT ON CONSTRAINT attributes_animal_id_attribute 
                                        DO UPDATE SET 
                                            status = EXCLUDED.status;'''.format(animal_id=animal_id,
                                                                                attributes=attributes))

                        cur.execute('''INSERT INTO animals.breed (animal_id, level, type) 
                                        SELECT '{animal_id}' as "animal_id", *
                                        FROM json_each_text('{breed}') 
                                        ON CONFLICT ON CONSTRAINT breed_animal_id_level 
                                        DO UPDATE SET 
                                            type = EXCLUDED.type;'''.format(animal_id=animal_id, breed=breed))

                        cur.execute('''INSERT INTO animals.colors (animal_id, level, type) 
                                        SELECT '{animal_id}' as "animal_id", *
                                        FROM json_each_text('{colors}') 
                                        ON CONFLICT ON CONSTRAINT colors_animal_id_level_uindex 
                                        DO UPDATE SET 
                                            type = EXCLUDED.type;'''.format(animal_id=animal_id, colors=colors))

                        cur.execute('''INSERT INTO animals.environment (animal_id, environment, status) 
                                        SELECT '{animal_id}' as "animal_id", *
                                        FROM json_each_text('{environment}') 
                                        ON CONFLICT ON CONSTRAINT environment_animal_id_environment_uindex 
                                        DO UPDATE SET 
                                            status = EXCLUDED.status;'''.format(animal_id=animal_id,
                                                                                environment=environment))

                        if len(photos) > 0:
                            for photo in photos:
                                cur.execute('''INSERT INTO animals.photos (animal_id, image_size, url) 
                                                SELECT '{animal_id}' as "animal_id", * 
                                                FROM json_each_text('{photo}') 
                                                ON CONFLICT ON CONSTRAINT photos_animal_id_image_size_uindex 
                                                DO NOTHING;'''.format(animal_id=animal_id, photo=json.dumps(photo)))

                        if len(videos) > 0:
                            for video in videos:
                                cur.execute('''INSERT INTO animals.videos (animal_id, type, source) 
                                                SELECT '{animal_id}' as "animal_id", *
                                                FROM json_each_text('{video}')
                                                ON CONFLICT ON CONSTRAINT videos_pk_2 
                                                DO NOTHING;'''.format(animal_id=animal_id, video=json.dumps(video)))
                        animals.append(animal)
                        print('animal inserted: ' + str(animal_id))
        except (RuntimeError, exceptions.PetfinderError) as err:
            print(err)

        s3_client.put_object(Body=json.dumps(animals),
                             Bucket='pethub-data',
                             Key='petfinder/animals/{org_id}_animals_{dt}.json'
                             .format(org_id=org, dt=datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')))

    conn.close()


def _create_connection():
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD")
    )

    return conn


if __name__ == '__main__':
    get_animals()

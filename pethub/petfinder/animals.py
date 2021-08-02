# -*- coding: utf-8 -*-
"""
The animals module contains the function for extracting, transforming and loading listed animals on Petfinder.com into
database tables and other storage endpoints.

Example
-------
The script can be ran manually by invoking the main call method:

    $ python animals.py

"""
from dotenv import load_dotenv
import json
import os
import sys
import petpy
from petpy import exceptions
import datetime
import boto3
import psycopg2


load_dotenv('../../.env')
"""Load the needed environment variables. These could also be system level variables if you are running the script 
on your local machine."""

pf = petpy.Petfinder(key=os.environ.get('PETFINDER_PETHUB_KEY'),
                     secret=os.environ.get('PETFINDER_PETHUB_SECRET_KEY'))
"""Initialized Petfinder class with authenticated connection. 

The environment variables PETFINDER_PETHUB_KEY and PETFINDER_PETHUB_SECRET_KEY are the access and secret keys obtained 
from the Petfinder API when registering for an API key.
"""

s3_session = boto3.Session(
    aws_access_key_id=os.environ.get('PETHUB_AWS_SECRET_ID'),
    aws_secret_access_key=os.environ.get('PETHUB_AWS_SECRET_ACCESS_KEY')
)
"""S3 session object for connecting and interacting with Amazon Web Services programmatically.

The environment variables PETHUB_AWS_SECRET_ID and PETHUB_AWS_SECRET_ACCESS_KEY are obtained from the Amazon Web 
Services IAM console."""
s3_client = s3_session.client("s3")
"""S3 client object which is used for performing the actual programmatic operations with AWS such as uploading files."""


def get_animals():
    r"""

    Returns
    -------
    None

    """
    # Create a connection to the Postgres database using psycopg2's connection class.
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD")
    )
    # Get the organization IDs stored in the `organization` table within the `organizations` schema and save the
    # IDs to a list.
    with conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT id 
                            FROM organizations.organization;''')
            organization_ids = [f[0] for f in cur.fetchall()]
    # For each organization in the list of organizations obtained from the database table, we get the currently listed
    # animals available on Petfinder.com and the API. The listed animal data at each animal welfare organization is
    # then transformed before the data is inserted into the appropriate tables in the `animals` schema.
    for org in organization_ids:
        # Because we are iterating over many more results than in the organizations module, begin a try/except
        # block to catch any possible exceptions that may arise from `petpy` and the API in general.
        try:
            # Get all the animals listed at the current organization.
            animals_at_orgs = pf.animals(organization_id=org, results_per_page=100, pages=None)
            # We only need the animals data from the returned JSON object, thus we take it from the dictionary
            # representation of the result from the Petfinder API.
            animals_at_orgs = animals_at_orgs['animals']
            # Before transforming and loading the animal data into the database tables, save the returned animal
            # data for each organization in S3.
            s3_client.put_object(Body=json.dumps(animals_at_orgs),
                                 Bucket='pethub-data',
                                 Key='petfinder/animals/{org_id}_animals_{dt}.json'
                                 .format(org_id=org, dt=datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')))

            # For each animal listed at the current organization, we extract and transform the returned data from the
            # API before inserting it into the database tables.
            for animal in animals_at_orgs:
                # Save the animal ID as a variable as it will be used throughout the loop.
                animal_id = animal['id']
                # Using list comprehension, get the fields we want to insert into the `animal` table.
                ani = dict((k, animal[k]) for k in ['id', 'organization_id', 'url', 'type', 'species', 'age',
                                                    'gender', 'size', 'coat', 'tags', 'name', 'description']
                           if k in animal)
                # The dictionary representation of the JSON data is then transformed into a string representation and
                # any single quotes are escaped with double single-quotes.
                ani = json.dumps(ani).replace("'", "''")

                # Get the status related fields for the `animal_status` table and add the animal ID to the dictionary.
                ani_status = dict((k, animal[k]) for k in ['status', 'status_changed_at', 'published_at']
                                  if k in animal)
                ani_status['animal_id'] = animal_id
                # Dump the JSON (dictionary) as a string.
                ani_status = json.dumps(ani_status)
                # Dump the nested JSON data into strings for the `attributes`, `breed`, `colors` and `environment`
                # tables.
                attributes = json.dumps(animal['attributes'])
                breed = json.dumps(animal['breeds'])
                colors = json.dumps(animal['colors'])
                environment = json.dumps(animal['environment'])
                # The photos and videos are nested lists that we will iterate over and insert individually.
                photos = animal['photos']
                videos = animal['videos']

                # Begin a database transaction using the psycopg2 connection context manager. If any of the SQL
                # statements within the transaction fail, all the statements are rolled back to their previous state
                # to maintain data consistency.
                with conn:
                    with conn.cursor() as cur:
                        # Insert the transformed animal data from above into the respective tables. To maintain data
                        # consistency and to help avoid duplicates, we use an update-insert (upsert) strategy that
                        # updates any animal record already in the database table.
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
                        # If the photos and videos fields contains any values in the nested lists, iterate over them
                        # and insert the values into the respective tables.
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
                        # Print a success message that the animal record has been added to the database.
                        print('animal inserted: ' + str(animal_id))
        # If a RunTimeError or an error in the PetfinderError class is raised, print the error and exit the script.
        except (RuntimeError, exceptions.PetfinderError) as err:
            print(err)
            sys.exit()
        except KeyError:
            raise exceptions.PetfinderError('Rate Limit Reached')

    # Close the created connection
    conn.close()


if __name__ == '__main__':
    get_animals()

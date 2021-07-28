# -*- coding: utf-8 -*-
"""
The organizations module contains the functions necessary for extracting organization level data from the Petfinder
API and storing the data into the appropriate locations.

Example
-------
The script can be ran manually by invoking the main call method:

    $ python organizations.py

"""

from dotenv import load_dotenv
import json
import os
import petpy
import datetime
import boto3
import psycopg2


load_dotenv('../../../.env')
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


def get_organizations():
    r"""
    Primary function for extracting and loading animal welfare organizations in the United States.

    Returns
    ------
    None

    See Also
    --------
    petpy
        https://github.com/aschleg/petpy
    psycopg2
        https://www.psycopg.org/docs/
    json_each_text, json_populate_record
        https://www.postgresql.org/docs/10/functions-json.html

    """
    # Create a connection to the Postgres database that stores the extracted Petfinder API data.
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD")
    )
    # With the authenticated Petfinder API connection established, call the `organizations` function in `petpy`
    # to get approximately 10,000 animal welfare organizations across the United States. The Petfinder API only allows
    # 1,000 calls per day, so this call will likely use most, if not all, of the day's allotted calls. The maximum
    # amount of results per page is 100 (defaults to 20).
    orgs = pf.organizations(country='US', results_per_page=100, pages=1000)
    # The returned object is a dictionary representing the returned JSON from the API. We are only interested in the
    # organization data from the returned information.
    orgs = orgs['organizations']
    # Before transforming and loading data from a data source, it is often recommended to store the raw results in a
    # file storage system such as S3. Saving the results allows us to keep the extracted data without needing to call
    # the API subsequent times. Also, with tools such as AWS Redshift, Athena and more, we can query the stored JSON
    # results directly if needed.
    s3_client.put_object(Body=json.dumps(orgs),
                         Bucket='pethub-data',
                         Key='petfinder/organizations/organizations_{dt}.json'
                         .format(dt=datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')))

    # The following loop goes through each organization extracted from the Petfinder API and applies any necessary
    # transformations to the JSON data and inserts the relevant fields into the appropriate database tables.
    for org in orgs:
        # Save the organization ID as a variable as it will be used frequently through the loop.
        org_id = org['id']
        # Get the organization fields from JSON data using list comprehension.
        organization = dict((k, org[k]) for k in ['id', 'name', 'email', 'phone', 'url', 'website',
                                                  'mission_statement'] if k in org)
        # Transform the organization JSON data into a string representation using the `json.dumps()` function. A
        # single quote in a field (data table row) will result in an error with Postgres. To avoid this, we also replace
        # any single quotes with double single-quotes as this acts as an escape character in Postgres.
        organization = json.dumps(organization).replace("'", "''")
        # Save the nested organization address data
        address = org['address']
        # Add the organization ID for it to be included in the final database table.
        address['org_id'] = org_id
        # The address JSON data is then transformed into a string representation using the `json.dumps()` function.
        # We also replace any single quotes with double-single quotes to escape any single quotes that may appear
        # in the data for it to be properly inserted into the database table.
        address = json.dumps(address).replace("'", "''")
        # Use the `json.dumps()` function again on the nested hours and social media information in the organization
        # level data.
        hours = json.dumps(org['hours'])
        social_media = json.dumps(org['social_media'])
        # The photo field is a nested list of dictionaries, therefore we iterate over the list to add the organization
        # ID before inserting the data into the table.
        photos = org['photos']
        for photo in photos:
            photo['org_id'] = org_id
        # The adoption policy and information of the organization is saved with the organization ID added and any
        # single quotes escaped as done above.
        adoption = org['adoption']
        adoption['org_id'] = org_id
        adoption = json.dumps(adoption).replace("'", "''")

        # The data is now ready to be inserted into the appropriate database tables. We create a transaction using
        # a context manager so in the event that if any of the SQL commands fail, the entire operation will be rolled
        # backed to help ensure data consistency.
        with conn:
            with conn.cursor() as cur:
                # Insert each transformed data object into their appropriate table using INSERT INTO ... SELECT *
                # syntax. We also try to maintain data consistency by using the table keys to find any currently
                # inserted organizations in the tables and update the fields with the new data.
                cur.execute('''INSERT INTO organizations.organization
                                SELECT * 
                                FROM json_populate_record(null::organizations.organization, '{organization}') 
                                ON CONFLICT ON CONSTRAINT organization_pk 
                                DO UPDATE SET 
                                    name = EXCLUDED.name, 
                                    email = EXCLUDED.email, 
                                    phone = EXCLUDED.phone, 
                                    url = EXCLUDED.url, 
                                    website = EXCLUDED.website, 
                                    mission_statement = EXCLUDED.mission_statement;'''
                            .format(organization=organization))
                cur.execute('''INSERT INTO organizations.address 
                                SELECT * 
                                FROM json_populate_record(null::organizations.address, '{address}') 
                                ON CONFLICT ON CONSTRAINT address_pk 
                                DO UPDATE SET 
                                    address1 = EXCLUDED.address1, 
                                    address2 = EXCLUDED.address2, 
                                    city = EXCLUDED.city, 
                                    state = EXCLUDED.state, 
                                    postcode = EXCLUDED.postcode, 
                                    country = EXCLUDED.country;'''.format(address=address))
                cur.execute('''INSERT INTO organizations.hours (org_id, day_of_week, hours) 
                                SELECT '{org_id}' as "org_id", * 
                                FROM json_each_text('{hours}')
                                ON CONFLICT ON CONSTRAINT hours_org_id_day_of_week 
                                DO UPDATE SET 
                                    hours = EXCLUDED.hours;'''.format(org_id=org_id, hours=hours))
                cur.execute('''INSERT INTO organizations.social_media (org_id, site, url)
                                SELECT '{org_id}' as "org_id", * 
                                FROM json_each_text('{social_media}') 
                                ON CONFLICT ON CONSTRAINT social_media_org_id_site 
                                DO UPDATE SET 
                                    url = EXCLUDED.url;'''.format(org_id=org_id, social_media=social_media))
                cur.execute('''INSERT INTO organizations.adoption
                                                SELECT *
                                                FROM json_populate_record(null::organizations.adoption, '{adoption}') 
                                                ON CONFLICT ON CONSTRAINT adoption_policy_pk 
                                                DO UPDATE SET 
                                                    policy = EXCLUDED.policy, 
                                                    url = EXCLUDED.url;'''
                            .format(adoption=adoption))

                for photo in photos:
                    cur.execute('''INSERT INTO organizations.photos (org_id, image_size, url) 
                                    SELECT '{org_id}' as "org_id", * 
                                    FROM json_each_text('{photos}')'''.format(org_id=org_id, photos=json.dumps(photo)))
        # Print to console that the inserts into the organizations tables was successful.
        print('organization inserted: ' + org_id)
    # Lastly, close the created connection.
    conn.close()


if __name__ == '__main__':
    get_organizations()

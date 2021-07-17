from dotenv import load_dotenv
import json
import os
import petpy
import datetime
import concurrent.futures
import boto3
import psycopg2


load_dotenv('../../../.env')


class PetfinderETL(object):

    def __init__(self):
        self.petfinder_access_key = os.environ.get('PETFINDER_PETHUB_KEY')
        self.petfinder_secret_key = os.environ.get('PETFINDER_PETHUB_SECRET_KEY')
        self.pf = petpy.Petfinder(key=self.petfinder_access_key,
                                  secret=self.petfinder_secret_key)
        self.s3_session = boto3.Session(
            aws_access_key_id=os.environ.get('PETHUB_AWS_SECRET_ID'),
            aws_secret_access_key=os.environ.get('PETHUB_AWS_SECRET_ACCESS_KEY')
        )
        self.s3_client = self.s3_session.client("s3")

    def get_organizations(self):
        conn = self._create_connection()
        # TODO: If possible, edit getting organizations to exclude those already included in the DB
        orgs = self.pf.organizations(country='US', results_per_page=100, pages=10000)
        orgs = orgs['organizations']

        # TODO: Multiprocess the loop below
        for org in orgs:
            organization = dict((k, org[k]) for k in ['id', 'name', 'email', 'phone', 'url', 'website',
                                                      'mission_statement'] if k in org)
            org_id = organization['id']

            address = org['address']
            address['org_id'] = org_id
            address = json.dumps(address)
            address = address.replace("'", "''")

            hours = org['hours']
            hours = json.dumps(hours)

            social_media = org['social_media']
            social_media = json.dumps(social_media)

            photos = org['photos']
            for photo in photos:
                photo['org_id'] = org_id

            adoption = org['adoption']
            adoption['org_id'] = org_id
            adoption = json.dumps(adoption)
            adoption = adoption.replace("'", "''")

            organization = json.dumps(organization)
            organization = organization.replace("'", "''")

            with conn:
                with conn.cursor() as cur:
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
                                        mission_statement = EXCLUDED.mission_statement;'''.format(organization=organization))
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
                                        hours = EXCLUDED.hours;'''
                                .format(org_id=org_id, hours=hours))

                    cur.execute('''INSERT INTO organizations.social_media (org_id, site, url)
                                    SELECT '{org_id}' as "org_id", * 
                                    FROM json_each_text('{social_media}') 
                                    ON CONFLICT ON CONSTRAINT social_media_org_id_site 
                                    DO UPDATE SET 
                                        url = EXCLUDED.url;'''
                                .format(org_id=org_id, social_media=social_media))

                    for photo in photos:
                        cur.execute('''INSERT INTO organizations.photos (org_id, image_size, url) 
                                        SELECT '{org_id}' as "org_id", * 
                                        FROM json_each_text('{photos}')'''
                                    .format(org_id=org_id, photos=json.dumps(photo)))

                    cur.execute('''INSERT INTO organizations.adoption
                                    SELECT *
                                    FROM json_populate_record(null::organizations.adoption, '{adoption}') 
                                    ON CONFLICT ON CONSTRAINT adoption_policy_pk 
                                    DO UPDATE SET 
                                        policy = EXCLUDED.policy, 
                                        url = EXCLUDED.url;'''
                                .format(adoption=adoption))

            print('organization inserted: ' + org_id)

        self.s3_client.put_object(Body=json.dumps(orgs),
                                  Bucket='pethub-data',
                                  Key='petfinder/organizations/organizations_{dt}.json'
                                  .format(dt=datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')))

    @staticmethod
    def _create_connection():
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
        )

        return conn


if __name__ == '__main__':
    petfinder_etl = PetfinderETL()
    petfinder_etl.get_organizations()

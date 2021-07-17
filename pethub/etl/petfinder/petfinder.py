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

    def get_organizations(self):
        # TODO: If possible, edit getting organizations to exclude those already included in the DB
        orgs = self.pf.organizations(location='WA', country='US', results_per_page=100, pages=100)['organizations']

        # TODO: Multiprocess the loop below
        for org in orgs:
            organization = dict((k, org[k]) for k in ['id', 'name', 'email', 'phone', 'url', 'website',
                                                      'mission_statement'] if k in org)
            address = org['address']
            hours = org['hours']
            social_media = org['social_media']
            photos = org['photos']

        # TODO: Edit to upload to specified path in S3
        with open('organizations_{date}.json'.format(date=datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')), 'w') as f:
            json.dump(orgs['organizations'], f)

import requests
import pandas as pd


outcomes_endpoint = 'https://data.austintexas.gov/resource/9t4d-g238.json'
intakes_endpoint = 'https://data.austintexas.gov/resource/fdzn-9yqv.json'


def get_soda_api_data(endpoint, count=1000, offset=0, return_df=True):
    params = {'$limit': count, '$offset': offset}

    results = []

    while True:

        try:
            r = requests.get(endpoint, params=params)

        except requests.exceptions.HTTPError as err:

            if err.response.status_code == '404':
                break

        finally:
            rcontent = r.json()

            if rcontent:
                break

            results.append(rcontent)
            offset += count
            params['$offset'] = offset

    if return_df:
        results_df = []

        for i in results:
            results_df.append(i)

        results = pd.DataFrame.from_records(results_df)

    return results


if __name__ == '__main__':
    outcomes_df = get_soda_api_data(outcomes_endpoint)
    intakes_df = get_soda_api_data(intakes_endpoint)

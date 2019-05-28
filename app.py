import boto3
import json
import time
import requests
import logging
import datetime
import sys
from botocore.exceptions import ClientError

logger = logging.getLogger('scale_reports')

logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter)
logger.addHandler(sh)

fh = logging.FileHandler(r'scalereports.log', mode='a')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)


def main(argv):
    if len(argv) != 8:
        print(f'You have {len(argv)} argument(s), but the script needs 8: start_cluster, end_cluster, start_boost, '
              f'interval, num_intervals, alg_id, query_set, user_profile')
        sys.exit(1)

    start_cluster = int(argv[0])
    end_cluster = int(argv[1])
    start_boost = int(argv[2])
    boost_interval = int(argv[3])
    num_intervals = int(argv[4])
    alg_id = int(argv[5])
    query_set = int(argv[6])
    user_profile = argv[7]

    validate_input(start_cluster, end_cluster, user_profile)

    max_boost = start_boost + num_intervals * boost_interval
    if max_boost == start_boost:
        max_boost = max_boost + 1

    if boost_interval == 0:
        boost_interval = 1

    clusters = range(start_cluster, end_cluster + 1)

    session = boto3.Session(
        aws_access_key_id='',
        aws_secret_access_key='')

    s3_client = session.client('s3')

    available_workers = 2
    s3_filenames = []

    for cluster in clusters:
        for boost in range(start_boost, max_boost, boost_interval):
            if available_workers > 0:
                response = create_scale_report(boost, cluster, alg_id, query_set, user_profile);
                if response.status_code != 200:
                    logger.error(f'Error code {response.status_code} from SAW for cluster {cluster}, boost {boost}')
                else:
                    filename = f'c{cluster}-b{boost}.zip'
                    logger.info(f'Report created. {filename} added to list of currently processing reports.')
                    s3_filenames.append(filename)
                    available_workers = available_workers - 1

            while available_workers == 0:
                logger.debug('Waiting 30 seconds...')
                time.sleep(30)
                for file in s3_filenames:
                    logger.debug(f'Searching S3 for {file}...')
                    try:

                        key = s3_client.head_object(Bucket='getty-search-usw2-stage-saw',
                                                    IfModifiedSince=get_modified_time(),
                                                    Key=f'report-data/{file}')

                        if key is not None:
                            cluster_id = file[1:file.index('-')]
                            boost_value = file[file.index('-') + 2:file.index('.')]
                            logger.info(f'**** Report for cluster {cluster_id}, boost {boost_value} completed. *****')
                            s3_filenames.remove(file)
                            available_workers = available_workers + 1

                    except ClientError:
                        logger.debug(f'File {file} not found yet.')

    logger.info('All scale reports created. You may have to wait for the last ones to complete.')


def create_scale_report(boost, cluster_id, alg_id, query_set, user_profile):
    saw_url = "http://usw2-stage-search-saw.lower-getty.cloud/runscalereport"
    body = create_body(boost, cluster_id, alg_id, query_set, user_profile)
    headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*'
    }
    return requests.post(saw_url, body, headers=headers)


def create_body(boost_value, cluster_id, alg_id, query_set, user_profile):
    style_cluster_boost = "{\"boost\":" + str(boost_value) + "," + "\"clusters\":" + "\"" + str(cluster_id) + ",0\"}"

    return json.dumps({
        "reportId": -1,
        "pageSize": 100,
        "pieRecencyMaxBrandNew": 4,
        "pieRecencyMaxFresh": 52,
        "pieRelevancyMaxLowConfidence": 2,
        "pieGlobalRelevancyMaxLowConfidence": 2,
        "pieCalculatedRelevancyMaxLowConfidence": 2,
        "pieKeywordLowImpression": 28.25,
        "pieKeywordHighImpression": 141.25,
        "pieTrueLowImpression": 28.25,
        "pieTrueHighImpression": 141.25,
        "pieAssetLowImpression": 14.12,
        "pieAssetHighImpression": 70.6,
        "pieLowDownloadRatio": 40,
        "pieHighDownloadRatio": 200,
        "avgRelevancyScoreFilterOutliers": False,
        "recencyScoreFilterOutliers": False,
        "querySetType": "Standard",
        "querySetId": str(query_set),
        "querySetStartTime": get_date(),
        "querySetDuration": 1,
        "querySetTargetSearchMix": None,
        "algorithmIds": [alg_id],
        "pageNo": 1,
        "userProfile": user_profile,
        "solrFarmsByAlgorithmIds": {str(alg_id): "false"},
        "contentScopes": ["getty"],
        "styleClusterBoost": style_cluster_boost
    }, separators=(',', ':'))


def validate_input(start_cluster, end_cluster, user_profile):
    user_profiles = ["RF", "RF_PLUS"]

    if start_cluster < 0 or end_cluster > 499:
        print('Invalid cluster value. Valid cluster values are between 0 and 499, inclusive.')
        sys.exit(1)
    if start_cluster > end_cluster:
        print('Invalid cluster value. Starting cluster value should not be greater than the ending cluster value.')
        sys.exit(1)
    if user_profile not in user_profiles:
        print(f'Invalid user profile {user_profile}. Valid values are {user_profiles}')
        sys.exit(1)


def get_date():
    return (datetime.datetime.now() - datetime.timedelta(days=1)).replace(microsecond=0).strftime("%Y-%m-%d %H:00:00")


def get_modified_time():
    return (datetime.datetime.utcnow() - datetime.timedelta(minutes=2)).replace(microsecond=0)


if __name__ == "__main__":
    main(sys.argv[1:])

import json
import os

from flask import Flask, send_file, request, Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from scheme import DiagnosisKey, Base
from configuration import Configuration

assert 'CONFIG_PATH' in os.environ, 'Env "CONFIG_PATH" must be set.'

config_path = os.environ['CONFIG_PATH']

assert os.path.exists(config_path), 'Config path %s is not exist.' % config_path

config = None
with open(config_path, mode='r') as fp:
    config = Configuration(json.load(fp))

if not os.path.exists(config.base_path):
    os.makedirs(config.base_path)

engine = create_engine(
    config.db_uri,
    encoding="utf-8",
    echo=True
)

Base.metadata.create_all(bind=engine)

app = Flask(__name__)


def _create_session():
    return scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )
    )


def _is_valid_cluster_id(cluster_id):
    if len(cluster_id) != 6:
        return False
    if not str.isnumeric(cluster_id):
        return False

    return True


MIMETYPE_JSON = 'application/json'
MIMETYPE_ZIP = 'application/zip'


@app.route("/diagnosis_keys/<cluster_id>/list.json", methods=['GET'])
def diagnosis_keys_index(cluster_id):
    if not _is_valid_cluster_id(cluster_id):
        return "[]"

    zip_store_path = os.path.join(config.base_path, cluster_id)
    if not os.path.exists(zip_store_path):
        return "[]"

    filtered_zip_list = list(filter(lambda f: f.endswith('.zip'), os.listdir(zip_store_path)))

    item_list = []

    for file_name in filtered_zip_list:
        url = os.path.join(config.base_url, cluster_id, file_name)
        path = os.path.join(config.base_path, cluster_id, file_name)
        timestamp = int(os.stat(path).st_mtime)
        item = {'region': config.region, 'url': url, 'created': timestamp}
        item_list.append(item)
    json_str = json.dumps(item_list, indent=4)

    return Response(response=json_str,
                    status=200,
                    mimetype=MIMETYPE_JSON)


@app.route("/diagnosis_keys/<cluster_id>/<zip_file_name>", methods=['GET'])
def diagnosis_keys(cluster_id, zip_file_name):
    if not _is_valid_cluster_id(cluster_id):
        return "ClusterID:%s invalid" % cluster_id

    zip_file_path = os.path.join(config.base_path, cluster_id, zip_file_name)
    if not os.path.exists(zip_file_path):
        return "ClusterID:%s, %s not found" % (cluster_id, zip_file_name), 404

    return send_file(zip_file_path,
                     as_attachment=True,
                     attachment_filename=zip_file_name,
                     mimetype=MIMETYPE_ZIP)


def _convert_to_diagnosis_key(json_obj, cluster_id):
    diagnosis_key = DiagnosisKey()
    diagnosis_key.cluster_id = cluster_id
    diagnosis_key.key = json_obj['key']
    diagnosis_key.reportType = json_obj['reportType']
    diagnosis_key.rollingStartNumber = json_obj['rollingStartNumber']
    diagnosis_key.rollingPeriod = json_obj['rollingPeriod']
    diagnosis_key.transmissionRisk = json_obj['transmissionRisk']
    return diagnosis_key


def _is_exists(session, cluster_id, diagnosis_key):
    return session.query(DiagnosisKey) \
               .filter(DiagnosisKey.cluster_id == cluster_id) \
               .filter(DiagnosisKey.key == diagnosis_key.key) \
               .count() > 0


@app.route("/diagnosis_keys/<cluster_id>/<file_name>", methods=['PUT'])
def put_diagnosis_keys(cluster_id, file_name):
    if not _is_valid_cluster_id(cluster_id):
        return "ClusterID:%s invalid" % cluster_id

    data = request.get_data()
    json_obj = json.loads(data)
    key_list = json_obj['temporaryExposureKeys']
    diagnosis_keys = list(map(lambda obj: _convert_to_diagnosis_key(obj, cluster_id), key_list))

    session = _create_session()

    filtered_diagnosis_keys = list(
        filter(lambda diagnosis_key: not _is_exists(session, cluster_id, diagnosis_key), diagnosis_keys)
    )

    try:
        session.bulk_save_objects(filtered_diagnosis_keys)
        session.commit()
    finally:
        session.close()

    count = len(filtered_diagnosis_keys)
    return '%d diagnosis_keys have been added.' % count, 200  # OK

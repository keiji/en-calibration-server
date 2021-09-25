import json
import os
from datetime import datetime
from http import HTTPStatus

from flask import Flask, send_file, request, Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from common import convert_to_diagnosis_key, is_exists, FORMAT_SYMPTOM_ONSET_DATE
from scheme import Base
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
                    status=HTTPStatus.OK,
                    mimetype=MIMETYPE_JSON)


@app.route("/diagnosis_keys/<cluster_id>/<zip_file_name>", methods=['GET'])
def diagnosis_keys(cluster_id, zip_file_name):
    if not _is_valid_cluster_id(cluster_id):
        return "ClusterID:%s invalid" % cluster_id

    zip_file_path = os.path.join(config.base_path, cluster_id, zip_file_name)
    if not os.path.exists(zip_file_path):
        return "ClusterID:%s, %s not found" % (cluster_id, zip_file_name), HTTPStatus.NOT_FOUND

    return send_file(zip_file_path,
                     as_attachment=True,
                     attachment_filename=zip_file_name,
                     mimetype=MIMETYPE_ZIP)


@app.route("/diagnosis_keys/<cluster_id>/<file_name>", methods=['PUT'])
def put_diagnosis_keys(cluster_id, file_name):
    if not _is_valid_cluster_id(cluster_id):
        return "ClusterID:%s invalid" % cluster_id

    data = request.get_data()
    json_obj = json.loads(data)

    idempotency_key = json_obj['idempotencyKey']
    symptom_onset_date_str = json_obj['symptomOnsetDate']
    symptom_onset_date = datetime.strptime(symptom_onset_date_str, FORMAT_SYMPTOM_ONSET_DATE)
    key_list = json_obj['temporaryExposureKeys']

    try:
        diagnosis_keys = list(map(lambda obj: convert_to_diagnosis_key(
            obj,
            cluster_id,
            symptom_onset_date,
            idempotency_key
        ), key_list))
    except KeyError as e:
        return '', HTTPStatus.BAD_REQUEST

    session = _create_session()

    filtered_diagnosis_keys = list(
        filter(lambda diagnosis_key: not is_exists(session, cluster_id, diagnosis_key), diagnosis_keys)
    )

    try:
        session.bulk_save_objects(filtered_diagnosis_keys)
        session.commit()
    finally:
        session.close()

    response_diagnosis_keys \
        = list(map(lambda diagnosis_key: diagnosis_key.to_serializable_object(), filtered_diagnosis_keys))

    return Response(
        response=json.dumps(response_diagnosis_keys),
        status=HTTPStatus.OK,
        mimetype='application/json'
    )

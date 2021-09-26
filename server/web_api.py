import hashlib
import json
import os
from datetime import datetime
from http import HTTPStatus
import uuid

from flask import Flask, send_file, request, Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from common import convert_to_diagnosis_key, is_exists, FORMAT_RFC3339
from scheme import Base
from configuration import Configuration
from sorter import sort_daily_summaries, sort_exposure_windows, sort_exposure_informations

DIAGNOSIS_KEYS_DIR = 'diagnosis_keys'
EXPOSURE_DATA_DIR = 'exposure_data'

MAXIMUM_CONTENT_LENGTH = 1024 * 1024 * 20  # 20MiB

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

    zip_store_path = os.path.join(config.base_path, cluster_id, DIAGNOSIS_KEYS_DIR)
    if not os.path.exists(zip_store_path):
        return "[]"

    filtered_zip_list = list(filter(lambda f: f.endswith('.zip'), os.listdir(zip_store_path)))

    item_list = []

    for file_name in filtered_zip_list:
        url = os.path.join(config.base_url, DIAGNOSIS_KEYS_DIR, cluster_id, file_name)
        path = os.path.join(config.base_path, cluster_id, DIAGNOSIS_KEYS_DIR, file_name)
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

    zip_file_path = os.path.join(config.base_path, cluster_id, DIAGNOSIS_KEYS_DIR, zip_file_name)
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

    idempotency_key = uuid.uuid4().hex if 'idempotencyKey' not in json_obj else json_obj['idempotencyKey']
    symptom_onset_date_str = json_obj['symptomOnsetDate']
    symptom_onset_date = datetime.strptime(symptom_onset_date_str, FORMAT_RFC3339)
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
        mimetype=MIMETYPE_JSON
    )


@app.route("/exposure_data/<cluster_id>/list.json", methods=['GET'])
def exposure_data_index(cluster_id):
    if not _is_valid_cluster_id(cluster_id):
        return "[]"

    json_store_path = os.path.join(config.base_path, cluster_id, EXPOSURE_DATA_DIR)
    if not os.path.exists(json_store_path):
        return "[]"

    filtered_json_list = list(filter(lambda f: f.endswith('.json'), os.listdir(json_store_path)))

    item_list = []

    for file_name in filtered_json_list:
        url = os.path.join(config.base_url, EXPOSURE_DATA_DIR, cluster_id, file_name)
        path = os.path.join(config.base_path, cluster_id, EXPOSURE_DATA_DIR, file_name)
        timestamp = int(os.stat(path).st_mtime)
        item = {'url': url, 'created': timestamp}
        item_list.append(item)

    item_list = sorted(item_list, key=lambda item: item['created'], reverse=True)

    json_str = json.dumps(item_list, indent=4)

    return Response(
        response=json_str,
        status=HTTPStatus.OK,
        mimetype=MIMETYPE_JSON
    )


@app.route("/exposure_data/<cluster_id>/<file_name>", methods=['GET'])
def exposure_data(cluster_id, file_name):
    if not _is_valid_cluster_id(cluster_id):
        return "ClusterID:%s invalid" % cluster_id

    file_path = os.path.join(config.base_path, cluster_id, EXPOSURE_DATA_DIR, file_name)
    if not os.path.exists(file_path):
        return "ClusterID:%s, %s not found" % (cluster_id, file_name), HTTPStatus.NOT_FOUND

    with open(file_path, 'r') as fp:
        content = fp.read()
        return Response(
            response=content,
            status=HTTPStatus.OK,
            mimetype=MIMETYPE_JSON
        )


def _is_valid_exposure_data(exposure_data):
    if not 'en_version' in exposure_data:
        return False
    if not 'exposure_configuration' in exposure_data:
        return False
    if not (
            ('exposure_summary' in exposure_data and 'exposure_informations' in exposure_data)
            or
            ('daily_summaries' in exposure_data and 'exposure_windows' in exposure_data)
    ):
        return False

    return True


def _get_file_name(json_obj):
    json_str = json.dumps(json_obj)
    sha256 = hashlib.sha256()
    sha256.update(json_str.encode('UTF-8'))

    return '%s.json' % sha256.hexdigest()


@app.route("/exposure_data/<cluster_id>/", methods=['PUT'])
def put_exposure_data(cluster_id):
    if not _is_valid_cluster_id(cluster_id):
        return "ClusterID:%s invalid" % cluster_id

    if request.content_length > MAXIMUM_CONTENT_LENGTH:
        return Response(
            response='{}',
            status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            mimetype=MIMETYPE_JSON
        )

    data = request.get_data()
    json_obj = json.loads(data)

    # Sort
    json_obj['exposure_informations'] = sort_exposure_informations(json_obj['exposure_informations'])
    json_obj['daily_summaries'] = sort_daily_summaries(json_obj['daily_summaries'])
    json_obj['exposure_windows'] = sort_exposure_windows(json_obj['exposure_windows'])

    if not _is_valid_exposure_data(json_obj):
        return Response(
            response='{}',
            status=HTTPStatus.BAD_REQUEST,
            mimetype=MIMETYPE_JSON
        )

    output_dir = os.path.join(config.base_path, str(cluster_id), EXPOSURE_DATA_DIR)
    os.makedirs(output_dir, exist_ok=True)

    file_name = _get_file_name(json_obj)

    json_obj['file_name'] = file_name
    json_obj['url'] = os.path.join(config.base_url, EXPOSURE_DATA_DIR, cluster_id, file_name)

    file_path = os.path.join(output_dir, file_name)
    if os.path.exists(file_path):
        return Response(
            response=json.dumps(json_obj),
            status=HTTPStatus.OK,
            mimetype=MIMETYPE_JSON
        )

    with open(file_path, mode='w') as fp:
        json.dump(json_obj, fp, indent=4)

    return Response(
        response=json.dumps(json_obj),
        status=HTTPStatus.CREATED,
        mimetype=MIMETYPE_JSON
    )

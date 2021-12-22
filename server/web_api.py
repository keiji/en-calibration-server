import hashlib
import json
import os
from datetime import datetime, timezone, timedelta
from http import HTTPStatus
import uuid
import csv

from flask import Flask, send_file, request, Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from common import convert_to_diagnosis_key, is_exists, FORMAT_RFC3339
from scheme import Base
from configuration import Configuration
from sorter import sort_daily_summaries, sort_exposure_windows, sort_exposure_informations

JST = timezone(timedelta(hours=9), 'Asia/Tokyo')

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


MIMETYPE_JSON = 'application/json'
MIMETYPE_ZIP = 'application/zip'
MIMETYPE_CSV = 'text/csv'


@app.route("/diagnosis_keys/<region>/list.json", methods=['GET'])
def _diagnosis_keys_index(region):
    return diagnosis_keys_index(region, '')


@app.route("/diagnosis_keys/<region>/<sub_region>/list.json", methods=['GET'])
def diagnosis_keys_index(region, sub_region):
    zip_store_path = os.path.join(config.base_path, region, sub_region, DIAGNOSIS_KEYS_DIR)
    if not os.path.exists(zip_store_path):
        return "[]"

    filtered_zip_list = list(filter(lambda f: f.endswith('.zip'), os.listdir(zip_store_path)))

    item_list = []

    for file_name in filtered_zip_list:
        url = os.path.join(config.base_url, DIAGNOSIS_KEYS_DIR, region, sub_region, file_name)
        path = os.path.join(config.base_path, region, sub_region, DIAGNOSIS_KEYS_DIR, file_name)
        created_timestamp = os.stat(path).st_mtime
        created_datetime = datetime.fromtimestamp(created_timestamp).astimezone(JST)
        item = {
            'region': region,
            'sub_region': sub_region,
            'url': url,
            'created': int(created_timestamp),
            'datetime': created_datetime.strftime(FORMAT_RFC3339)
        }
        item_list.append(item)
    json_str = json.dumps(item_list, indent=4)

    return Response(response=json_str,
                    status=HTTPStatus.OK,
                    mimetype=MIMETYPE_JSON)


@app.route("/diagnosis_keys/<region>/<zip_file_name>", methods=['GET'])
def _diagnosis_keys(region, zip_file_name):
    return diagnosis_keys(region, '', zip_file_name)


@app.route("/diagnosis_keys/<region>/<sub_region>/<zip_file_name>", methods=['GET'])
def diagnosis_keys(region, sub_region, zip_file_name):
    zip_file_path = os.path.join(config.base_path, region, sub_region, DIAGNOSIS_KEYS_DIR, zip_file_name)
    if not os.path.exists(zip_file_path):
        return "Region:%s, SubRegion %s, %s not found" % (region, sub_region, zip_file_name), HTTPStatus.NOT_FOUND

    return send_file(zip_file_path,
                     as_attachment=True,
                     attachment_filename=zip_file_name,
                     mimetype=MIMETYPE_ZIP)


@app.route("/diagnosis_keys/<file_name>", methods=['PUT'])
def put_diagnosis_keys(file_name):
    data = request.get_data()
    json_obj = json.loads(data)

    idempotency_key = uuid.uuid4().hex if 'idempotencyKey' not in json_obj else json_obj['idempotencyKey']
    symptom_onset_date_str = json_obj['symptomOnsetDate']
    symptom_onset_date = datetime.strptime(symptom_onset_date_str, FORMAT_RFC3339)

    regions = json_obj['regions']

    sub_regions = []
    if 'sub_regions' in json_obj:
        sub_regions = json_obj['sub_regions']

    # Region Level
    sub_regions.append('')

    key_list = json_obj['keys']

    diagnosis_keys = []

    try:
        for region in regions:
            keys = list(map(lambda obj: convert_to_diagnosis_key(
                obj,
                str(region),
                None,
                symptom_onset_date,
                idempotency_key
            ), key_list))
            diagnosis_keys.extend(keys)

            for sub_region in sub_regions:
                keys = list(map(lambda obj: convert_to_diagnosis_key(
                    obj,
                    str(region),
                    str(sub_region),
                    symptom_onset_date,
                    idempotency_key
                ), key_list))
                diagnosis_keys.extend(keys)
    except KeyError as e:
        return '', HTTPStatus.BAD_REQUEST

    session = _create_session()

    filtered_diagnosis_keys = list(
        filter(lambda diagnosis_key: not is_exists(session, diagnosis_key), diagnosis_keys)
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


@app.route("/exposure_data/<region>/list.json", methods=['GET'])
def _exposure_data_index(region):
    return exposure_data_index(region, '')


@app.route("/exposure_data/<region>/<sub_region>/list.json", methods=['GET'])
def exposure_data_index(region, sub_region):
    json_store_path = os.path.join(config.base_path, region, sub_region, EXPOSURE_DATA_DIR)
    if not os.path.exists(json_store_path):
        return "[]"

    filtered_json_list = list(filter(lambda f: f.endswith('.json'), os.listdir(json_store_path)))

    item_list = []

    for file_name in filtered_json_list:
        identifier, _ = os.path.splitext(file_name)
        json_url = os.path.join(config.base_url, EXPOSURE_DATA_DIR, region, sub_region, file_name)
        exposure_windows_csv_url = os.path.join(config.base_url, EXPOSURE_DATA_DIR, region, sub_region, identifier,
                                                "exposure_windows.csv")
        daily_summaries_csv_url = os.path.join(config.base_url, EXPOSURE_DATA_DIR, region, sub_region, identifier,
                                               "daily_summaries.csv")
        path = os.path.join(config.base_path, region, sub_region, EXPOSURE_DATA_DIR, file_name)
        created_timestamp = os.stat(path).st_mtime
        created_datetime = datetime.fromtimestamp(created_timestamp).astimezone(JST)
        item = {
            'url': json_url,
            'exposure_windows_csv_url': exposure_windows_csv_url,
            'daily_summaries_csv_url': daily_summaries_csv_url,
            'created': int(created_timestamp),
            'datetime': created_datetime.strftime(FORMAT_RFC3339)
        }
        item_list.append(item)

    item_list = sorted(item_list, key=lambda item: item['created'], reverse=True)

    json_str = json.dumps(item_list, indent=4)

    return Response(
        response=json_str,
        status=HTTPStatus.OK,
        mimetype=MIMETYPE_JSON
    )


@app.route("/exposure_data/<region>/<file_name>", methods=['GET'])
def _exposure_data(region, file_name):
    return exposure_data(region, '', file_name)


@app.route("/exposure_data/<region>/<sub_region>/<file_name>", methods=['GET'])
def exposure_data(region, sub_region, file_name):
    file_path = os.path.join(config.base_path, region, sub_region, EXPOSURE_DATA_DIR, file_name)
    if not os.path.exists(file_path):
        return "Region:%s, SubRegion:%s, %s not found" % (region, sub_region, file_name), HTTPStatus.NOT_FOUND

    with open(file_path, 'r') as fp:
        content = fp.read()
        return Response(
            response=content,
            status=HTTPStatus.OK,
            mimetype=MIMETYPE_JSON
        )


def _convert_exposure_windows_to_csv(region, sub_region, identifier, json_obj):
    exposure_windows = json_obj['exposure_windows']

    if exposure_windows is None:
        return "", HTTPStatus.NOT_FOUND

    file_name = "%s-exposure_windows.csv" % identifier
    file_path = os.path.join(config.base_path, region, sub_region, EXPOSURE_DATA_DIR, file_name)
    if not os.path.exists(file_path):
        with open(file_path, mode='w') as fp:
            writer = csv.writer(fp)
            writer.writerow(
                [
                    "CalibrationConfidence", "DateMillisSinceEpoch", "WindowNumber", "Infectiousness", "ReportType",
                    "MinAttenuationDb", "SecondsSinceLastScan", "TypicalAttenuationDb"
                ])

            for window_number, ew in enumerate(exposure_windows):
                calibrationConfidence = ew["CalibrationConfidence"]
                dateMillisSinceEpoch = ew["DateMillisSinceEpoch"]
                infectiousness = ew["Infectiousness"]
                reportType = ew["ReportType"]
                for si in ew["ScanInstances"]:
                    writer.writerow([
                        calibrationConfidence, dateMillisSinceEpoch, window_number, infectiousness, reportType,
                        si["MinAttenuationDb"], si["SecondsSinceLastScan"], si["TypicalAttenuationDb"],
                    ])

    return send_file(
        file_path,
        as_attachment=True,
        attachment_filename=file_name,
        mimetype=MIMETYPE_CSV
    )


def _write_csv_row(dateMillisSinceEpoch, daily_summary, type, csv_writer):
    summary = daily_summary[type]
    if summary is not None:
        csv_writer.writerow([
            dateMillisSinceEpoch,
            type,
            summary["MaximumScore"],
            summary["ScoreSum"],
            summary["WeightedDurationSum"]
        ])


def _convert_daily_summaries_to_csv(region, sub_region, identifier, json_obj):
    daily_summaries = json_obj['daily_summaries']

    if daily_summaries is None:
        return "", HTTPStatus.NOT_FOUND

    file_name = "%s-daily_summaries.csv" % identifier
    file_path = os.path.join(config.base_path, region, sub_region, EXPOSURE_DATA_DIR, file_name)
    if not os.path.exists(file_path):
        with open(file_path, mode='w') as fp:
            writer = csv.writer(fp)
            writer.writerow(["DateMillisSinceEpoch", "Type", "MaximumScore", "ScoreSum", "WeightedDurationSum"])

            for ds in daily_summaries:
                dateMillisSinceEpoch = ds["DateMillisSinceEpoch"]
                _write_csv_row(dateMillisSinceEpoch, ds, "DaySummary", writer)
                _write_csv_row(dateMillisSinceEpoch, ds, "ConfirmedClinicalDiagnosisSummary", writer)
                _write_csv_row(dateMillisSinceEpoch, ds, "ConfirmedTestSummary", writer)
                _write_csv_row(dateMillisSinceEpoch, ds, "RecursiveSummary", writer)
                _write_csv_row(dateMillisSinceEpoch, ds, "SelfReportedSummary", writer)

    return send_file(
        file_path,
        as_attachment=True,
        attachment_filename=file_name,
        mimetype=MIMETYPE_CSV
    )


@app.route("/exposure_data/<region>/<identifier>/<type>", methods=['GET'])
def _exposure_data_detail(region, identifier, type):
    return exposure_data_detail(region, '', identifier, type)


@app.route("/exposure_data/<region>/<sub_region>/<identifier>/<type>", methods=['GET'])
def exposure_data_detail(region, sub_region, identifier, type):
    file_path = os.path.join(config.base_path, region, sub_region, EXPOSURE_DATA_DIR, "%s.json" % identifier)
    if not os.path.exists(file_path):
        return "", HTTPStatus.NOT_FOUND

    with open(file_path, 'r') as fp:
        json_obj = json.load(fp)

        if type == 'exposure_windows.csv':
            return _convert_exposure_windows_to_csv(region, sub_region, identifier, json_obj)
        elif type == 'daily_summaries.csv':
            return _convert_daily_summaries_to_csv(region, sub_region, identifier, json_obj)


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


def _get_identifier(json_obj):
    json_str = json.dumps(json_obj)
    sha256 = hashlib.sha256()
    sha256.update(json_str.encode('UTF-8'))

    return sha256.hexdigest()


@app.route("/exposure_data/<region>", methods=['PUT'], strict_slashes=False)
def _put_exposure_data(region):
    return put_exposure_data(region, '')


@app.route("/exposure_data/<region>/<sub_region>", methods=['PUT'], strict_slashes=False)
def put_exposure_data(region, sub_region):
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

    output_dir = os.path.join(config.base_path, region, sub_region, EXPOSURE_DATA_DIR)
    os.makedirs(output_dir, exist_ok=True)

    identifier = _get_identifier(json_obj)
    file_name = "%s.json" % identifier

    json_obj['file_name'] = file_name
    json_obj['url'] = os.path.join(config.base_url, EXPOSURE_DATA_DIR, region, sub_region, file_name)

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

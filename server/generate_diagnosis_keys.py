import base64
import hashlib
import json
import os
import sys
import tempfile
import zipfile

import temporary_exposure_key_export_pb2 as tek

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from ecdsa import SigningKey

from scheme import Base, DiagnosisKey
from configuration import Configuration

HEADER = "EK Export v1    "
HEADER_BYTES = HEADER.encode(encoding='utf-8')

VERIFICATION_KEY_VERSION = 'v1'
SIGNATURE_ALGORITHM = '1.2.840.10045.4.3.2'

FILENAME_BIN = 'export.bin'
FILENAME_SIG = 'export.sig'


def _setup_signature_info(signature_info, verification_key_id):
    signature_info.verification_key_id = str(verification_key_id)
    signature_info.verification_key_version = VERIFICATION_KEY_VERSION
    signature_info.signature_algorithm = SIGNATURE_ALGORITHM
    return signature_info


def _setup_key(diagnosis_key, key):
    # https://developers.google.com/android/exposure-notifications/exposure-key-file-format
    key.key_data = base64.b64decode(diagnosis_key.key.encode())
    key.transmission_risk_level = diagnosis_key.transmissionRisk
    key.rolling_start_interval_number = diagnosis_key.rollingStartNumber
    key.rolling_period = diagnosis_key.rollingPeriod
    key.report_type = diagnosis_key.reportType
    key.days_since_onset_of_symptoms = diagnosis_key.daysSinceOnsetOfSymptoms

    print(key.key_data.hex())
    print(key.days_since_onset_of_symptoms)

    return key


def _export_generate(cluster_id, verification_id, diagnosis_keys, output_dir, batch_num=1, batch_size=1):
    output_path = os.path.join(output_dir, FILENAME_BIN)

    tekObj = tek.TemporaryExposureKeyExport()
    tekObj.start_timestamp = min(map(lambda dk: dk.createdAt, diagnosis_keys))
    tekObj.end_timestamp = max(map(lambda dk: dk.createdAt, diagnosis_keys))
    tekObj.region = cluster_id
    tekObj.batch_num = batch_num
    tekObj.batch_size = batch_size

    for dk in diagnosis_keys:
        _ = _setup_key(dk, tekObj.keys.add())

    signature_info = tekObj.signature_infos.add()
    _setup_signature_info(signature_info, verification_id)

    with open(output_path, 'wb') as fp:
        fp.write(HEADER_BYTES)
        fp.write(tekObj.SerializeToString())

    return output_path


def _export_tek_signs(export_bin_path, verification_id, signing_key, output_dir, batch_num=1, batch_size=1):
    output_path = os.path.join(output_dir, FILENAME_SIG)

    tekSignList = tek.TEKSignatureList()

    tekSignature = tekSignList.signatures.add()
    _setup_signature_info(tekSignature.signature_info, verification_id)
    tekSignature.batch_num = batch_num
    tekSignature.batch_size = batch_size

    with open(export_bin_path, mode='rb') as fp:
        bytes = fp.read()
        signature = signing_key.sign(bytes)
        tekSignature.signature = signature
        print(signature.hex())

    with open(output_path, 'wb') as fp:
        fp.write(tekSignList.SerializeToString())

    return output_path


def _compress_zip(export_bin_path, export_sig_path, output_dir):
    fd, output_path = tempfile.mkstemp(prefix='diagnosis_keys-', suffix='.zip', dir=output_dir)
    os.close(fd)

    with zipfile.ZipFile(output_path, mode='w', compression=zipfile.ZIP_DEFLATED) as zip:
        zip.write(export_bin_path, arcname=FILENAME_BIN)
        zip.write(export_sig_path, arcname=FILENAME_SIG)

    return output_path


def export_diagnosis_keys(config):
    assert os.path.exists(config.base_path), '%s not exists' % config.base_path

    fp = open(config.signing_key_path)
    signing_key = SigningKey.from_pem(fp.read(), hashlib.sha256)
    fp.close()

    engine = create_engine(
        config.db_uri,
        encoding="utf-8",
        echo=True)

    Base.metadata.create_all(bind=engine)

    session = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )
    )

    try:
        cluster_objs = session.query(DiagnosisKey.cluster_id, DiagnosisKey.exported) \
            .filter(DiagnosisKey.exported == False) \
            .distinct() \
            .all()

        if len(cluster_objs) == 0:
            print('No updated-cluster found.')
            return

        print('%d updated-cluster found.' % len(cluster_objs))

        for obj in cluster_objs:
            cluster_id = obj.cluster_id
            output_dir = os.path.join(config.base_path, str(cluster_id))
            os.makedirs(output_dir, exist_ok=True)

            diagnosis_keys = session.query(DiagnosisKey) \
                .filter(DiagnosisKey.cluster_id == cluster_id) \
                .filter(DiagnosisKey.exported == False) \
                .all()

            print('%d new diagnosis-keys have been found.' % len(diagnosis_keys))

            export_bin_path = _export_generate(cluster_id, config.region, diagnosis_keys, output_dir)
            export_sig_path = _export_tek_signs(export_bin_path, config.region, signing_key, output_dir)
            export_zip_path = _compress_zip(export_bin_path, export_sig_path, output_dir)

            os.remove(export_bin_path)
            os.remove(export_sig_path)

            for diagnosis_key in diagnosis_keys:
                diagnosis_key.exported = True
            session.commit()

            print("export_completed: %s" % export_zip_path)

    finally:
        session.close()


def main(argv):
    assert 'CONFIG_PATH' in os.environ, 'Env "CONFIG_PATH" must be set.'

    config_path = os.environ['CONFIG_PATH']

    assert os.path.exists(config_path), 'Config path %s is not exist.' % config_path

    config = None
    with open(config_path, mode='r') as fp:
        config = Configuration(json.load(fp))

    export_diagnosis_keys(config)


if __name__ == '__main__':
    main(sys.argv)

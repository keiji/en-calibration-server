import json
import os
import subprocess
import sys
import tempfile
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from scheme import Base, DiagnosisKey
from configuration import Configuration


def _convert_to_json_obj(diagnosis_key):
    return {
        'key': diagnosis_key.key,
        'rollingStartNumber': diagnosis_key.rollingStartNumber,
        'rollingPeriod': diagnosis_key.rollingPeriod,
        'reportType': diagnosis_key.reportType,
        'transmissionRisk': diagnosis_key.transmissionRisk
    }


def _run_export_generate(config, json_file_path, output_dir):
    base_name = os.path.basename(json_file_path)
    name, _ = os.path.splitext(base_name)

    filename_prefix = name + '-'
    file_path = os.path.join(output_dir, filename_prefix)

    subprocess.run([config.export_generate_bin_path,
                    '-signing-key', config.signing_key_path,
                    '-filename-root', file_path,
                    '-key-version', "2",
                    '-tek-file', json_file_path,
                    ])


def export_diagnosis_keys(config):
    assert os.path.exists(config.base_path), '%s not exists' % config.base_path

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

            json_list = list(map(lambda diagnosis_key: _convert_to_json_obj(diagnosis_key), diagnosis_keys))
            json_obj = json.dumps({
                'temporaryExposureKeys': json_list,
                'created': int(time.time())
            }, indent=4)

            output_dir = os.path.join(config.base_path, str(cluster_id))
            os.makedirs(output_dir, exist_ok=True)

            fd, json_file_path = tempfile.mkstemp(prefix='diagnosis_keys-', suffix='.json', dir=output_dir)
            os.close(fd)

            with open(json_file_path, mode='w+') as fp:
                fp.write(json_obj)

            _run_export_generate(config, json_file_path, output_dir)

            for diagnosis_key in diagnosis_keys:
                diagnosis_key.exported = True
            session.commit()

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

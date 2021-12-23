import json
import os
import time
from random import Random
from datetime import datetime

from absl import app
from absl import flags

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from common import convert_to_diagnosis_key, is_exists, FORMAT_RFC3339
from scheme import Base

FLAGS = flags.FLAGS
flags.DEFINE_string("db_path", "./test.db", "Database path")
flags.DEFINE_string("input_json_path", "./sample/diagnosis_keys.json", "Sample JSON path")

MAX_DELAY_IN_SEC = 2

def main(argv):
    del argv  # Unused.

    assert os.path.exists(FLAGS.input_json_path), '%s not exists' % FLAGS.input_json_path

    db_uri = "sqlite:///%s" % FLAGS.db_path
    engine = create_engine(
        db_uri,
        encoding="utf-8",
        echo=True)

    Base.metadata.create_all(bind=engine)

    with open(FLAGS.input_json_path, encoding="utf-8", mode='r') as fp:
        data = fp.read()

    rand = Random()

    json_obj = json.loads(data)
    idempotency_key = json_obj['idempotencyKey']
    symptom_onset_date_str = json_obj['symptomOnsetDate']
    symptom_onset_date = datetime.strptime(symptom_onset_date_str, FORMAT_RFC3339)

    regions = json_obj['regions']
    sub_regions = json_obj['sub_regions']

    # Region level
    sub_regions.append('')

    key_list = json_obj['keys']

    diagnosis_keys = []
    for region in regions:
        for sub_region in sub_regions:
            for key in key_list:
                time.sleep(rand.random() * MAX_DELAY_IN_SEC)
                diagnosis_keys.append(convert_to_diagnosis_key(key, region, sub_region, symptom_onset_date, idempotency_key))

    session = scoped_session(
        sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )
    )

    filtered_diagnosis_keys = list(
        filter(lambda diagnosis_key: not is_exists(session, diagnosis_key), diagnosis_keys)
    )

    try:
        session.bulk_save_objects(filtered_diagnosis_keys)
        session.commit()
    finally:
        session.close()
        print('Test data generated.')


if __name__ == '__main__':
    app.run(main)

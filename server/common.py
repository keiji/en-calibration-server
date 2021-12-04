from datetime import datetime, timezone
import time

from scheme import DiagnosisKey

# RFC3339
FORMAT_RFC3339 = "%Y-%m-%dT%H:%M:%S.%f%z"

TIMEWINDOW_IN_SEC = 60 * 10
DEFAULT_TRANSMISSION_RISK = 4


def convert_to_diagnosis_key(json_obj, region, sub_region, symptom_onset_date, idempotency_key):
    diagnosis_key = DiagnosisKey()
    diagnosis_key.region = region
    diagnosis_key.sub_region = sub_region
    diagnosis_key.key = json_obj['key']
    diagnosis_key.reportType = json_obj['reportType']
    diagnosis_key.rollingStartNumber = json_obj['rollingStartNumber']
    diagnosis_key.rollingPeriod = json_obj['rollingPeriod']
    diagnosis_key.transmissionRisk = DEFAULT_TRANSMISSION_RISK
    diagnosis_key.createdAt = int(time.time())

    diagnosis_key.daysSinceOnsetOfSymptoms = \
        _calc_days_since_onset_of_symptoms(diagnosis_key.rollingStartNumber, symptom_onset_date)
    diagnosis_key.primary_key = _gen_primary_key(idempotency_key, diagnosis_key)

    return diagnosis_key


def _calc_days_since_onset_of_symptoms(rolling_start_number, symptom_onset_date):
    rolling_start_epoch = rolling_start_number * TIMEWINDOW_IN_SEC
    rolling_start_date = datetime.fromtimestamp(rolling_start_epoch, timezone.utc)

    days_since_onset_of_symptoms = rolling_start_date - symptom_onset_date

    return days_since_onset_of_symptoms.days


def _gen_primary_key(idempotency_key, diagnosis_key):
    return ','.join([
        idempotency_key,
        str(diagnosis_key.region),
        str(diagnosis_key.sub_region),
        diagnosis_key.key,
        str(diagnosis_key.rollingStartNumber),
        str(diagnosis_key.rollingPeriod)
    ])


def is_exists(session, diagnosis_key):
    return session.query(DiagnosisKey) \
               .filter(DiagnosisKey.region == diagnosis_key.region) \
               .filter(DiagnosisKey.sub_region == diagnosis_key.sub_region) \
               .filter(DiagnosisKey.key == diagnosis_key.key) \
               .count() > 0

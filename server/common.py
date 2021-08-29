import time

from scheme import DiagnosisKey

# https://developers.google.com/android/reference/com/google/android/gms/nearby/exposurenotification/TemporaryExposureKey#DAYS_SINCE_ONSET_OF_SYMPTOMS_UNKNOWN
DAYS_SINCE_ONSET_OF_SYMPTOMS_UNKNOWN = 2147483647


def convert_to_diagnosis_key(json_obj, cluster_id):
    diagnosis_key = DiagnosisKey()
    diagnosis_key.cluster_id = cluster_id
    diagnosis_key.key = json_obj['key']
    diagnosis_key.reportType = json_obj['reportType']
    diagnosis_key.rollingStartNumber = json_obj['rollingStartNumber']
    diagnosis_key.rollingPeriod = json_obj['rollingPeriod']
    diagnosis_key.transmissionRisk = json_obj['transmissionRisk']
    if 'daysSinceOnsetOfSymptoms' in json_obj:
        diagnosis_key.daysSinceOnsetOfSymptoms = json_obj['daysSinceOnsetOfSymptoms']
    else:
        diagnosis_key.daysSinceOnsetOfSymptoms = DAYS_SINCE_ONSET_OF_SYMPTOMS_UNKNOWN

    diagnosis_key.createdAt = int(time.time())

    return diagnosis_key


def is_exists(session, cluster_id, diagnosis_key):
    return session.query(DiagnosisKey) \
               .filter(DiagnosisKey.cluster_id == cluster_id) \
               .filter(DiagnosisKey.key == diagnosis_key.key) \
               .count() > 0

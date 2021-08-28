import time

from scheme import DiagnosisKey


def convert_to_diagnosis_key(json_obj, cluster_id):
    diagnosis_key = DiagnosisKey()
    diagnosis_key.cluster_id = cluster_id
    diagnosis_key.key = json_obj['key']
    diagnosis_key.reportType = json_obj['reportType']
    diagnosis_key.rollingStartNumber = json_obj['rollingStartNumber']
    diagnosis_key.rollingPeriod = json_obj['rollingPeriod']
    diagnosis_key.transmissionRisk = json_obj['transmissionRisk']
    diagnosis_key.daysSinceOnsetOfSymptoms = json_obj['daysSinceOnsetOfSymptoms']
    diagnosis_key.createdAt = json_obj['created']
    return diagnosis_key


def is_exists(session, cluster_id, diagnosis_key):
    return session.query(DiagnosisKey) \
               .filter(DiagnosisKey.cluster_id == cluster_id) \
               .filter(DiagnosisKey.key == diagnosis_key.key) \
               .count() > 0

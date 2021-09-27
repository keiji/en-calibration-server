from functools import cmp_to_key


def sort_exposure_informations(exposure_informations):
    if exposure_informations is None:
        return exposure_informations

    return sorted(exposure_informations, key=lambda ei: ei['DateMillisSinceEpoch'])


def sort_daily_summaries(daily_summaries):
    if daily_summaries is None:
        return daily_summaries

    return sorted(daily_summaries, key=lambda ds: ds['DateMillisSinceEpoch'])


def sort_exposure_windows(exposure_windows):
    if exposure_windows is None:
        return exposure_windows

    for ew in exposure_windows:
        ew['ScanInstances'] = sort_scan_instances(ew['ScanInstances'])

    def compare(l, r):
        if l['DateMillisSinceEpoch'] < r['DateMillisSinceEpoch']:
            return 1
        if l['DateMillisSinceEpoch'] > r['DateMillisSinceEpoch']:
            return -1

        l_scan_instances_length = len(l['ScanInstances'])
        r_scan_instances_length = len(r['ScanInstances'])
        if l_scan_instances_length < r_scan_instances_length:
            return 1
        if l_scan_instances_length > r_scan_instances_length:
            return -1

        l_total_scan_instances_min_attenuation_db = sum(map(lambda si: si['MinAttenuationDb'], l['ScanInstances']))
        r_total_scan_instances_min_attenuation_db = sum(map(lambda si: si['MinAttenuationDb'], r['ScanInstances']))
        if l_total_scan_instances_min_attenuation_db < r_total_scan_instances_min_attenuation_db:
            return 1
        if l_total_scan_instances_min_attenuation_db > r_total_scan_instances_min_attenuation_db:
            return -1

        l_typical_attenuationDb = sum(map(lambda si: si['TypicalAttenuationDb'], l['ScanInstances']))
        r_typical_attenuationDb = sum(map(lambda si: si['TypicalAttenuationDb'], r['ScanInstances']))
        if l_typical_attenuationDb < r_typical_attenuationDb:
            return 1
        if l_typical_attenuationDb > r_typical_attenuationDb:
            return -1

        l_seconds_since_last_scan = sum(map(lambda si: si['SecondsSinceLastScan'], l['ScanInstances']))
        r_seconds_since_last_scan = sum(map(lambda si: si['SecondsSinceLastScan'], r['ScanInstances']))
        if l_seconds_since_last_scan < r_seconds_since_last_scan:
            return 1
        if l_seconds_since_last_scan > r_seconds_since_last_scan:
            return -1

        return 0

    return sorted(exposure_windows, key=cmp_to_key(compare))


def sort_scan_instances(scan_instances):
    if scan_instances is None:
        return scan_instances

    def compare(l, r):
        if l['MinAttenuationDb'] < r['MinAttenuationDb']:
            return 1
        if l['MinAttenuationDb'] > r['MinAttenuationDb']:
            return -1
        if l['SecondsSinceLastScan'] < r['SecondsSinceLastScan']:
            return 1
        if l['SecondsSinceLastScan'] > r['SecondsSinceLastScan']:
            return -1
        if l['TypicalAttenuationDb'] < r['TypicalAttenuationDb']:
            return 1
        if l['TypicalAttenuationDb'] > r['TypicalAttenuationDb']:
            return -1
        return 0

    return sorted(scan_instances, key=cmp_to_key(compare))

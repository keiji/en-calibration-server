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
    return sorted(exposure_windows, key=lambda ew: ew['DateMillisSinceEpoch'])


def sort_scan_instances(scan_instances):
    if scan_instances is None:
        return scan_instances

    return sorted(scan_instances,
                  key=lambda si: (si['MinAttenuationDb'] + si['SecondsSinceLastScan'] + si['TypicalAttenuationDb']))

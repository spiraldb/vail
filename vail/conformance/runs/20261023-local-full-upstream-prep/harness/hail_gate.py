def should_run_hail(vail_result):
    return vail_result.get('status') == 'passed'

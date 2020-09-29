def generate_request(plugin_service, api_endpoint, api_key, group_id, instance_id, start_time, end_time, manually=True):
    request_sample = {}
    request_sample['groupId'] = group_id
    request_sample['apiEndpoint'] = api_endpoint
    request_sample['apiKey'] = api_key

    group_detail = plugin_service.tsanaclient.get_group_detail(api_endpoint, api_key, group_id)

    app_exist = False
    request_sample['seriesSets'] = group_detail['seriesSets']
    for app in group_detail['appInstances']:
        if app['instanceId'] == instance_id:
            app_exist = True
            request_sample['instance'] = app
            break
    
    if not app_exist:
        raise Exception('App instance {} not exist in group {}'.format(instance_id, group_id))

    
    request_sample['startTime'] = start_time
    request_sample['endTime'] = end_time
    request_sample['manually'] = manually

    return request_sample
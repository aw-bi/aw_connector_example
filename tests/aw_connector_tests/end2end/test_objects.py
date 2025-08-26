def test_objects(app_client):
    """ """
    r = app_client.post(
        url='data-source/objects',
        json={
            'data_source': {
                'id': 1,
                'type': 'custom',
                'params': {'db': 'db1'},
                'extra': {},
            }
        },
    )

    assert r.is_success, r.text

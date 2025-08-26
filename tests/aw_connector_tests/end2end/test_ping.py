def test_ping(app_client):
    """ """
    r = app_client.post(
        url='data-source/ping',
        json={
            'id': 1,
            'type': 'custom',
            'params': {'db': 'db1'},
            'extra': {},
        }
    )

    assert r.is_success, r.text


def test_ping_no_id(app_client):
    """ """
    r = app_client.post(
        url='data-source/ping',
        json={
            'type': 'custom',
            'params': {'db': 'db1'},
            'extra': {},
        }
    )

    assert r.is_success, r.text


def test_ping_unavailable_data_source(app_client):
    r = app_client.post(
        url='data-source/ping',
        json={
            'type': 'custom',
            'params': {'db': 'db2'},
            'extra': {},
        }
    )

    assert not r.is_success, 'Успешный ответ на ping для несуществующего источника'
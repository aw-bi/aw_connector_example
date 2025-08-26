def test_object_meta(app_client):
    """ """
    r = app_client.post(
        url='data-source/object-meta',
        json={
            'data_source': {
                'id': 1,
                'type': 'custom',
                'params': {'db': 'db1'},
                'extra': {},
            },
            'object_name': 'public.table1'
        },
    )

    assert r.is_success, r.text
    assert r.json()['columns'], 'Нет столцов в метаданных объекта'


def test_object_meta_wrong_name(app_client):
    """ """
    r = app_client.post(
        url='data-source/object-meta',
        json={
            'data_source': {
                'id': 1,
                'type': 'custom',
                'params': {'db': 'db1'},
                'extra': {},
            },
            'object_name': 'table1'
        },
    )

    assert not r.is_success, 'Успешный ответ для несуществующей таблицы источника'
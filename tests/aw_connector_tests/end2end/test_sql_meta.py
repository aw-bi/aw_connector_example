def test_object_meta(app_client):
    """ """
    r = app_client.post(
        url='data-source/sql-meta',
        json={
            'data_source': {
                'id': 1,
                'type': 'custom',
                'params': {'db': 'db1'},
                'extra': {},
            },
            'sql_text': 'select * from table1'
        },
    )

    assert r.is_success, r.text
    assert r.json()['columns'], 'Нет столцов в метаданных объекта'

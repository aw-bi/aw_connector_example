def test_object_data(app_client):
    """
    """
    r = app_client.post(
        url='data-source/sql-object-data',
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
    
    object_data = r.json()['data']
    assert object_data and isinstance(object_data, list), 'Нет данных объекта'
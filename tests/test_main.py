from app import NeoConnector

conn = NeoConnector()
conn.driver.session().run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

data = [{
    'name': 'Alex',
    'sex': 'm',
    'age': 23,
    'posts': ['Super test post', 'test', 'twit']
    },
    {
        'name': 'Megan',
        'sex': 'f',
        'age': 34,
        'posts': ['post', 'test_1', ':)']
    },
    {
    'name': 'Bob',
    'sex': 'm',
    'age': 21,
    'posts': ['ahahha:D', 'LMFAO', 'wtf']
    }
]

groups_data = [{
    'name': 'test1'
    },
    {
    'name': 'test2'
    }
]

for i in data:
    conn.add_person(**i)

conn.add_connection('Alex', 'Megan')
conn.add_connection('Megan', 'Bob')

conn.add_subscriber('Bob', 'test1')
conn.add_subscriber('Megan', 'test1')
conn.add_subscriber('Alex', 'test2')


def test_get_names():
    assert conn.get_names() == ['Alex', 'Bob', 'Megan']



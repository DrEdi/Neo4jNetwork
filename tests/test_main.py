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

for i in groups_data:
    conn.add_group(**i)

conn.add_connection('Alex', 'Megan')
conn.add_connection('Megan', 'Bob')

conn.add_subscriber('Bob', 'test1')
conn.add_subscriber('Megan', 'test1')
conn.add_subscriber('Alex', 'test2')


def test_get_names():
    assert conn.get_names() == ['Alex', 'Bob', 'Megan']


def test_get_males_ordered_by_age():
    assert conn.get_males_ordered_by_age() == [{'Alex': 23}, {'Bob': 21}]


def test_get_person_friends():
    assert conn.get_person_friends('Bob') == ['Megan']


def test_get_friends_of_friends():
    assert conn.get_friends_of_friends('Bob') == ['Alex', 'Bob']


def test_get_user_info():
    assert conn.get_user_info() == [{'Alex': 1}, {'Bob': 1}, {'Megan': 2}]


def test_get_groups():
    assert conn.get_groups() == ['test1', 'test2']


def test_get_groups_of_person():
    assert conn.get_groups_of_person('Megan') == ['test1']


def test_get_count_of_subcsribers():
    assert conn.get_count_of_subcsribers() == [{'test1': 2}, {'test2': 1}]


def test_get_count_of_friends_groups():
    assert conn.get_count_of_friends_groups('Megan') == [2]


def test_get_user_posts():
    assert conn.get_user_posts('Megan') == [['post', 'test_1', ':)']]


def test_get_posts_more_than():
    assert conn.get_posts_more_than(10) == [['Super test post'], [], []]


def test_get_users_stat_by_posts():
    assert conn.get_users_stat_by_posts() == [{'Alex': 3}, {'Megan': 3},
                                              {'Bob': 3}]


def test_get_friends_friends_posts_by_given_p():
    assert conn.get_friends_friends_posts_by_given_p('Bob') == [
        {'Bob': ['ahahha:D', 'LMFAO', 'wtf']},
        {'Alex': ['Super test post', 'test', 'twit']}]

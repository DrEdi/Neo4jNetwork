from neo4j.v1 import GraphDatabase


class NeoConnector:
    """Class to connect and interact with DB."""

    def __init__(self, uri="bolt://localhost:7687"):
        """Init entity with new connectrion.

        :param uri: full uri to db containing host and port
        """
        self.driver = GraphDatabase.driver(
            uri)

    def __add_person(self, tx, name, sex, age, posts):
        """Add new user to network.

        :param tx: neo4j connection
        :param name:  name of new user

        """
        tx.run("MERGE (a:User { name: $name, sex: $sex, age: $age, posts: "
               "$posts})", name=name, sex=sex, age=age, posts=posts)

    def __add_group(self, tx, name):
        """Add new group to network.

        :param tx: neo4j connection
        :param name:  name of new group

        """
        tx.run("MERGE (a:Group { name: $name})", name=name)

    def __add_connection(self, tx, first, second):
        """Add new connection to network.

        :param tx: neo4j connection
        :param first:  first user name
        :param second: name of second user

        """
        tx.run("MATCH (a:User {name: $name}),(b:User {name: $friend_name}) "
               "MERGE (a)-[:KNOWS]->(b)", name=first, friend_name=second)
        tx.run("MATCH (a:User {name: $name}),(b:User {name: $friend_name}) "
               "MERGE (b)-[:KNOWS]->(a)", name=first, friend_name=second)

    def __add_subscriber(self, tx, user_name, group_name):
        """Add subscriber to given group.

        :param tx: neo4j connection
        :param user_name: name of user
        :param group_name: name of group

        """
        tx.run("MATCH (a:User {name: $name}),(b:Group {name: $group_name})"
               "  MERGE (a)-[:SUBSCRIBE]->(b)",
               name=user_name, group_name=group_name)

    def add_person(self, name, sex, age, posts):
        """Add new user to network.

        :param name:  name of new user

        """
        with self.driver.session() as session:
            session.write_transaction(self.__add_person, name, sex, age, posts)

    def add_group(self, name):
        """Add new group to network.

        :param name:  name of new group

        """
        with self.driver.session() as session:
            session.write_transaction(self.__add_group, name)

    def add_connection(self, first_user, second_user):
        """Add new connection to network.

        :param first_user:  first user name
        :param second_user: name of second user

        """
        with self.driver.session() as session:
            session.write_transaction(
                self.__add_connection, first_user, second_user)

    def add_subscriber(self, user_name, group_name):
        """Add subscriber to given group.

        :param user_name: name of user
        :param group_name: name of group

        """
        with self.driver.session() as session:
            session.write_transaction(
                self.__add_subscriber, user_name, group_name)

    def get_names(self):
        """Return list of person's name that are in network."""
        with self.driver.session() as session:
            data = session.run("MATCH (n:User) RETURN n.name AS name "
                               "ORDER BY n.name")
        return [i['name'] for i in data]

    def get_males_ordered_by_age(self):
        """Return list of dicts that contains name as key and age as value."""
        with self.driver.session() as session:
            info = session.run("MATCH (p:User) WHERE p.sex='m' RETURN p.name"
                               " AS name, p.age AS age ORDER BY p.age DESC")
        return [{i['name']: i['age']} for i in info]

    def get_person_friends(self, name):
        """Return list of names that have given person.

        :param name: name of person

        """
        with self.driver.session() as session:
            data = session.run("MATCH (node:User)<-[:KNOWS]-(n) WHERE "
                               "node.name = {x} RETURN n.name AS name ORDER BY"
                               " n.name", x=name)
        return [i['name'] for i in data]

    def get_friends_of_friends(self, name):
        """Return list of names of friends friends for given person.

        :param name: name of person

        """
        with self.driver.session() as session:
            data = session.run("MATCH (node:User)<-[:KNOWS]-(n)<-[:KNOWS]-(f)"
                               " WHERE node.name = {x} RETURN f.name AS name "
                               "ORDER BY f.name", x=name)
        return [i['name'] for i in data]

    def get_user_info(self):
        """Return count of friends for each user."""
        with self.driver.session() as session:
            data = session.run("MATCH (n:User)<-[:KNOWS]-(f) RETURN n.name AS"
                               " name, count(f) as count ORDER BY n.name")
        return [{i['name']: i['count']} for i in data]

    def get_groups(self):
        """Return list of groups name."""
        with self.driver.session() as session:
            data = session.run("MATCH (n: Group) RETURN n.name AS name "
                               "ORDER BY n.name")
        return [i['name'] for i in data]

    def get_groups_of_person(self, name):
        """Return list of groups for given person.

        :param name: name of person

        """
        with self.driver.session() as session:
            data = session.run("MATCH (g:Group)<-[:SUBSCRIBE]-(p:User) WHERE"
                               " p.name = {x} RETURN g.name as name ORDER BY "
                               "g.name", x=name)
        return [i['name'] for i in data]

    def get_count_of_subcsribers(self):
        """Return count of subscribers for each group."""
        with self.driver.session() as session:
            data = session.run("MATCH (g:Group)<-[:SUBSCRIBE]-(p:User) RETURN"
                               " g.name AS name, count(p) as count ORDER BY "
                               "count(p) DESC")
        return [{i['name']: i['count']} for i in data]

    def get_persons_with_groups(self):
        """Return count of subscribed groups for each person."""
        with self.driver.session() as session:
            data = session.run("MATCH (g:Group)<-[:SUBSCRIBE]-(p:User) RETURN"
                               " p.name AS name, count(g) AS count ORDER BY "
                               "count(g) DESC")
        return [{i['name']: i['count']} for i in data]

    def get_count_of_friends_groups(self, name):
        """Return return of groups for each friend for a given person.

        :param name: person name

        """
        with self.driver.session() as session:
            data = session.run("MATCH (p:User)<-[:KNOWS]-(f:User)<-[:KNOWS]-"
                               "(ff:User)-[:SUBSCRIBE]->(g:Group) WHERE "
                               "p.name={x} RETURN count(g) AS count", x=name)
        return [i['count'] for i in data]

    def get_user_posts(self, name):
        """Return list of posts for a given user.

        :param name:  name of user

        """
        with self.driver.session() as session:
            data = session.run("MATCH (n) WHERE n.name={x} RETURN n.posts AS "
                               "posts", x=name)
        return [i['posts'] for i in data]

    def get_posts_stat(self):
        """Return count of posts for each user."""
        with self.driver.session() as session:
            data = session.run("MATCH (p:User) WITH (reduce(total = 0,"
                               " ROW IN p.posts | total + length(row)))"
                               "/size(p.posts) AS num, p.name AS name RETURN "
                               "name, num ORDER BY num DESC")
        return [{i['name']: i['num']} for i in data]

    def get_posts_more_than(self, length):
        """Return list of posts that are longer that given number.

        :param length: int of length

        """
        with self.driver.session() as session:
            data = session.run("MATCH (n:User) RETURN filter(row in n.posts"
                               " WHERE length(row)> {x}) AS post", x=length)
        return [i['post'] for i in data]

    def get_users_stat_by_posts(self):
        """Return user middle length of posts for each user."""
        with self.driver.session() as session:
            data = session.run("MATCH (n:User) RETURN n.name AS name, "
                               "size(n.posts) as size ORDER BY size(n.posts)"
                               " DESC")
        return [{i['name']: i['size']} for i in data]

    def get_friends_friends_posts_by_given_p(self, name):
        """Return posts of friends friends for given user.

        :param name: name of user

        """
        with self.driver.session() as session:
            data = session.run("MATCH (n:User)<-[:KNOWS]-(f:User)<-[:KNOWS]"
                               "-(ff:User) WHERE n.name={x} RETURN ff.name AS"
                               " name,  ff.posts AS posts", x=name)
        return [{i['name']: i['posts']} for i in data]

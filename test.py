from cassandra.cluster import Cluster


def test_cassandra_connection(host):
    try:
        cluster = Cluster([host])
        session = cluster.connect()
        print(f"Successfully connected to Cassandra at {host}")
        session.shutdown()
        cluster.shutdown()
        return True
    except Exception as e:
        print(f"Connection to {host} failed: {str(e)}")
        return False


if __name__ == "__main__":
    hosts_to_test = ["localhost", "cassandra", "cassandra_db"]

    for host in hosts_to_test:
        print(f"Testing connection to {host}...")
        if test_cassandra_connection(host):
            print(f"host: '{host}'")
            break

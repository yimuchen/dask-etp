from dask_etp import ETPCondorCluster
from distributed import Client


if __name__ == "__main__":
    cluster = ETPCondorCluster()
    cluster.adapt(minimum=1, maximum=2)
    client = Client(cluster)

    client.wait_for_workers(1)


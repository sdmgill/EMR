from ManageEMR import build_emr_cluster, cluster_status, list_running_clusters

where = 'Dev'
done=build_emr_cluster(where)

cluster_status(done)


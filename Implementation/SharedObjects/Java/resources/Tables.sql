CREATE TABLE <LU_NAME>.get_jobs_failed_iids (
    iid text,
    failed_time timestamp,
    failure_reason text,
    full_error_msg text,
    PRIMARY KEY (iid, failed_time)
)


CREATE TABLE <LU_NAME>.iid_get_rate (
    uid text PRIMARY KEY,
    iid_avg_run_time_in_sec double,
    node text,
    status text,
    total_iid_failed bigint,
    total_iid_passed bigint,
    total_iid_processed bigint,
    total_iid_processed_in_ten_min bigint,
    update_time timestamp
)

CREATE TABLE <LU_NAME>.iid_stats (
    iid text,
    iid_sync_time timestamp,
    account_change_count bigint,
    cross_instance_check_count int,
    delete_orphan_check_count int,
    iid_change_ind_count bigint,
    iid_deltas_count int,
    iid_skipped boolean,
    replicates_requests_count int,
    replicates_send_to_kafka_due_to_delete_orphan_count bigint,
    replicates_send_to_kafka_due_to_replicate_request_count bigint,
    sync_duration bigint,
    time_to_execute_cross_instance_check bigint,
    time_to_execute_delete_orphan_check bigint,
    time_to_execute_replicates_request bigint,
    time_to_fetch_deltas_from_api bigint,
    time_to_fetch_deltas_from_iidf_queue_and_execute bigint,
    time_to_insert_deltas_to_iidfqueue_table bigint,
    PRIMARY KEY (iid, iid_sync_time)
)

CREATE TABLE <LU_NAME>.idfinder_stats (
    host text,
    operation_name text,
    iteration int,
    last_update_time timestamp,
    operation_value bigint,
    status text,
    PRIMARY KEY ((host, operation_name), iteration)
)

CREATE TABLE <LU_NAME>.lu_stats (
    job_run_seq bigint,
    operation_name text,
    last_update_time timestamp,
    operation_value text,
    status text,
    PRIMARY KEY (job_run_seq, operation_name, last_update_time)
)
CREATE TABLE k2view_customer.lu_stats ( 
    job_run_seq bigint, 
    operation_name text, 
    last_update_time timestamp, 
    operation_value text, 
    status text, 
    PRIMARY KEY (job_run_seq, operation_name, last_update_time) 
) WITH CLUSTERING ORDER BY (operation_name ASC, last_update_time ASC); 
 

 
CREATE TABLE k2view_customer.iid_stats ( 
    iid text, 
    iid_sync_time timestamp, 
    account_change_count bigint, 
    cross_instance_check_count int, 
    delete_orphan_check_count int, 
    iid_change_ind_count bigint, 
    iid_deltas_count int, 
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
iid_skipped boolean, 
    PRIMARY KEY (iid, iid_sync_time) 
 
);  

CREATE TABLE k2view_customer.idfinder_stats ( 
host text, 
operation_name text, 
iteration int, 
last_update_time timestamp, 
operation_value bigint, 
status text, 
PRIMARY KEY ((host, operation_name), iteration) 
);


CREATE TABLE k2view_customer.iid_get_rate (
node text,
uid text,
iid_avg_run_time_in_sec double,
status text,
total_iid_failed bigint,
total_iid_passed bigint,
total_iid_processed bigint,
total_iid_processed_in_ten_min bigint,
update_time timestamp,
PRIMARY KEY (uid)
);

CREATE TABLE k2view_customer.get_jobs_failed_iids (
    iid text,
    failed_time timestamp,
    failure_reason text,
    full_error_msg text,
    PRIMARY KEY (iid, failed_time)
) WITH CLUSTERING ORDER BY (failed_time ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 604800
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

create table k2view_customer.get_jobs_exceptions_iids(iid text primary key,failed_time timestamp,reason text);

CREATE KEYSPACE IF NOT EXISTS k2view_statistics WITH replication ={ 'class' : 'NetworkTopologyStrategy', 'DC1'  : 1 } AND durable_writes = true;
CREATE TABLE IF NOT EXISTS k2view_statistics.k2_statistics (
    conversation_id text,
    k2_statistics_id text,
    action text,
    additional_info text,
    elapsed_time_in_microseconds bigint,
    middle_tier_hostname text,
    otherinfo text,
    request_timestamp text,
    request_year_month text,
    requester_application_name text,
    requester_client_hostname text,
    requester_component_name text,
    requester_method_name text,
    requester_name text,
    response_timestamp text,
    server_component_name text,
    server_host_name text,
    server_method_name text,
    unique_transaction_id text,
    PRIMARY KEY (conversation_id, k2_statistics_id)
	) WITH CLUSTERING ORDER BY (k2_statistics_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 3888000
    AND gc_grace_seconds = 518400
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE IF NOT EXISTS k2view_statistics.k2_errors (
    conversation_id text,
    k2_statistics_id text,
    additional_info text,
    elapsed_time_in_microseconds bigint,
    error_message text,
    error_number text,
    error_severity text,
    error_timestamp text,
    line_number text,
    middle_tier_hostname text,
    otherinfo text,
    program_name text,
    program_owner text,
    request_timestamp text,
    request_year_month text,
    requester_application_name text,
    requester_client_hostname text,
    requester_component_name text,
    requester_method_name text,
    requester_name text,
    response_timestamp text,
    server_component_name text,
    server_host_name text,
    server_method_name text,
    unique_transaction_id text,
    PRIMARY KEY (conversation_id, k2_statistics_id)
	) WITH CLUSTERING ORDER BY (k2_statistics_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 3888000
    AND gc_grace_seconds = 518400
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE IF NOT EXISTS k2view_statistics.k2_statistics_report (
    request_timestamp text,
    server_method_name text,
    requester_name text,
    conversation_id text,
    k2_statistics_id text,
    elapsed_time_in_microseconds bigint,
    error_number text,
    PRIMARY KEY (request_timestamp, server_method_name, requester_name, conversation_id, k2_statistics_id)
	) WITH CLUSTERING ORDER BY (server_method_name ASC, requester_name ASC, conversation_id ASC, k2_statistics_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 3888000
    AND gc_grace_seconds = 518400
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

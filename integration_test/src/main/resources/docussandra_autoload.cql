create keyspace IF NOT EXISTS docussandra
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3};
//with replication = { 'class' : 'NetworkTopologyStrategy', 'use1' : 3};
use docussandra;

create table IF NOT EXISTS sys_meta (
	id text,
	version text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((id), updated_at)
)
with clustering order by (updated_at DESC);

create table IF NOT EXISTS sys_db (
	db_name text primary key,
	description text,
	created_at timestamp,
	updated_at timestamp
);

create table IF NOT EXISTS sys_tbl (
	db_name text,
	tbl_name text,
	description text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name)
);

create table IF NOT EXISTS sys_idx (
	db_name text,
	tbl_name text,
	name text,
	is_unique boolean,
	fields list<text>,
	fields_type list<text>,
	only list<text>,
    is_active boolean,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name, name)
);

create table IF NOT EXISTS sys_idx_status (
    id uuid,
	db_name text,
	tbl_name text,
	index_name text,
	records_completed bigint,
	total_records bigint,
	started_at timestamp,
	updated_at timestamp,
    errors list<text>,
    fatal_error text,
	primary key (id)
);

create table IF NOT EXISTS sys_idx_not_done (
    id uuid primary key
);

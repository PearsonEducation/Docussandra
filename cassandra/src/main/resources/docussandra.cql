drop keyspace if exists docussandra;
create keyspace docussandra
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use docussandra;

drop table if exists sys_meta;
create table sys_meta (
	id text,
	version text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((id), updated_at)
)
with clustering order by (updated_at DESC);

drop table if exists sys_db;
create table sys_db (
	db_name text primary key,
	description text,
	created_at timestamp,
	updated_at timestamp
);

drop table if exists sys_tbl;
create table sys_tbl (
	db_name text,
	tbl_name text,
	description text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name)
);

drop table if exists sys_idx;
create table sys_idx (
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

drop table if exists sys_idx_status;
create table sys_idx_status (
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

drop table if exists sys_idx_not_done;
create table sys_idx_not_done (
    id uuid primary key
);

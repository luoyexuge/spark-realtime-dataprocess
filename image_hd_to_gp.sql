\set ON_ERROR_STOP 1

set statement_timeout = 300000;

drop external table if exists staging.imageviews_hd_ext_:batchid;
create external table staging.imageviews_hd_ext_:batchid (
  id numeric(39,0),
  creative_id int,
  source text,
  imageview_date timestamp without time zone,
  date_i_raw int,
  referring_site text,
  opxsid text,
  opxpid text,
  view_type varchar(1),
  displayimage_id int,
  placement_id int,
  campaign_id int,
  client_id int,
  email text,
  useragent text,
  ip bigint,
  countryshort varchar(2),
  countrylong text,
  ipcity text,
  record_server bigint,
  ip_filter_flag int,
  robot_filter_flag int,
  valid_flag int,
  ht_score int
)
location (:hdfsloc)
format 'text' (null = '', escape = 'OFF')
log errors into staging.hd_ext_load_errors segment reject limit 10 rows;

create temporary table imageviews_hd_load (
    id numeric(39, 0) NOT NULL,
    creative_id integer NOT NULL,
    source text,
    date timestamp without time zone NOT NULL,
    date_i integer NOT NULL,
    timeslot smallint,
    record_server character varying(16),
    ip character varying(16),
    referring_site text,
    opxsid character varying(50),
    opxpid character varying(50),
    view_type character varying(1),
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    displayimage_id integer NOT NULL,
    placement_id integer NOT NULL,
    email character varying(100),
    dw_loaded_at timestamp without time zone DEFAULT now(),
    useragent character varying(500),
    ipcity character varying(44),
    countryshort character varying(2),
    ht_score integer

) DISTRIBUTED BY (opxpid);

insert into imageviews_hd_load (
  id,
  creative_id,
  source,
  date,
  date_i,
  record_server,
  ip,
  referring_site,
  opxsid,
  opxpid,
  view_type,
  created_at,
  updated_at,
  displayimage_id,
  placement_id,
  email,
  dw_loaded_at,
  useragent,
  ipcity,
  countryshort,
  ht_score
)
select
  id,
  creative_id,
  source,
  imageview_date,
  date_i_raw,
  host('0.0.0.0'::inet + record_server) record_server,
  host('0.0.0.0'::inet + ip) ip,
  referring_site,
  substr(opxsid, 1, 50),
  substr(opxpid, 1, 50),
  view_type,
  current_timestamp at time zone 'utc' created_at,
  current_timestamp at time zone 'utc' updated_at,
  displayimage_id,
  placement_id,
  substr(email, 1, 100),
  current_timestamp dw_loaded_at,
  substr(useragent, 1, 500),
  substr(ipcity, 1, 44),
  countryshort,
  ht_score
from staging.imageviews_hd_ext_:batchid
where valid_flag = 1;

insert into xmo_dw.imageviews
select * from imageviews_hd_load;

drop external table staging.imageviews_hd_ext_:batchid;


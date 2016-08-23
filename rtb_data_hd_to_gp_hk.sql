\set ON_ERROR_STOP 1

set statement_timeout = 300000;

drop external table if exists staging.rtb_datas_hd_ext_:batchid;
create external table staging.rtb_datas_hd_ext_:batchid (
  id numeric(39,0),
  bidded_at timestamp without time zone,
  viewed_at timestamp without time zone,
  viewed_at_i int,
  opxpid text,
  opxsid text,
  opxbid text,
  client_id int,
  pretargeting_id int,
  searchengine_id int,
  adgroup_id bigint,
  adtext_id bigint,
  placement_url text,
  winning_prize text,
  cpm varchar(64),
  ip bigint,
  useragent text,
  first_viewed_percent float,
  max_viewed_percent float,
  countryshort varchar(2),
  countrylong text,
  ipcity text,
  record_machine bigint,
  ip_filter_flag int,
  robot_filter_flag int,
  valid_flag int,
  aud_hash varchar(32),
  crm_hash varchar(32),
  adslot_id smallint,
  canonical text,
  domain text,
  rtb_cookie_id varchar(64),
  interest text,
  age varchar(50),
  gender varchar(50),
  audience varchar(50),
  reason text,
  tagid  varchar(500), --modified at 2016-03-04
  adx_name  character varying(20)  --modified at 2016-03-29
)
location (:hdfsloc)
format 'text' (null '' escape 'OFF' fill missing fields)
log errors into staging.hd_ext_load_errors segment reject limit 3000 rows;

create temporary table rtb_datas_hd_load (
    id numeric(39,0) NOT NULL,
    bidded_at timestamp without time zone NOT NULL,
    viewed_at timestamp without time zone NOT NULL,
    viewed_at_i integer NOT NULL,
    opxpid character varying(50),
    opxsid character varying(50),
    opxbid character varying(50),
    client_id integer,
    pretargeting_id int,
    searchengine_id integer DEFAULT 0,
    adgroup_id bigint,
    adtext_id bigint,
    placement_url character varying(500),
    winning_prize character varying(100),
    cpm integer,
    ip character varying(16),
    useragent varchar(500),
    first_viewed_percent float,
    max_viewed_percent float,
    countryshort varchar(2),
    countrylong varchar(44),
    ipcity varchar(44),
    record_machine character varying(16),
    aud_hash varchar(32),
    crm_hash varchar(32),
    dw_loaded_at timestamp without time zone DEFAULT now(),
    adslot_id smallint,
    canonical varchar(60),
    domain varchar(100),
    rtb_cookie_id varchar(64),
    ht_score int default 101,
    age varchar(10),
    gender varchar(10),
    audience varchar(50),
    reason varchar(500),
    interest varchar(500),
    tagid  varchar(500), --modified at 2016-03-04
    fraud  integer,          --modified at 2016-03-04
    adx_name  character varying(20)
) DISTRIBUTED BY (opxpid);


insert into rtb_datas_hd_load (
  id,
  bidded_at,
  viewed_at,
  viewed_at_i,
  opxpid,
  opxsid,
  opxbid,
  client_id,
  pretargeting_id,
  searchengine_id,
  adgroup_id,
  adtext_id,
  placement_url,
  winning_prize,
  cpm,
  ip,
  useragent,
  first_viewed_percent,
  max_viewed_percent,
  countryshort,
  countrylong,
  ipcity,
  record_machine,
  aud_hash,
  crm_hash,
  dw_loaded_at,
  adslot_id,
	canonical,
	domain,
  rtb_cookie_id,
  interest,
  age,
  gender,
  audience,
  reason,
  tagid ,--modified at 2016-03-04
 fraud, --modified at 2016-03-04
 adx_name
)
select
  id,
  bidded_at,
  viewed_at,
  viewed_at_i,
  substr(opxpid, 1, 50),
  substr(opxsid, 1, 50),
  substr(opxbid, 1, 50),
  client_id,
  pretargeting_id,
  searchengine_id,
  adgroup_id,
  adtext_id,
  substr(placement_url, 1, 500),
  substr(winning_prize, 1, 100),
  case when cpm::numeric > 2000000000 then 0 else cpm::integer end cpm,
  host('0.0.0.0'::inet + ip) ip,
  substr(useragent, 1, 500),
  first_viewed_percent,
  max_viewed_percent,
  countryshort,
  countrylong,
  ipcity,
  host('0.0.0.0'::inet + record_machine) record_machine,
  aud_hash,
  crm_hash,
  current_timestamp dw_loaded_at,
  adslot_id,
  substr(canonical, 1, 60),
  substr(domain, 1, 100),
  substr(rtb_cookie_id, 1, 64),
  substr(interest,1,500),
  substr(age,1,10),
  substr(gender,1,10),
  substr(audience,1,50),
  substr(reason,1,500),
  substr(tagid,1,500), --modified at 2016-03-04
  valid_flag  as fraud, --modified at 2016-03-04
  substr(adx_name, 1, 20)
from staging.rtb_datas_hd_ext_:batchid
where valid_flag in  (1,-32);

insert into xmo_dw.rtb_datas
select * from rtb_datas_hd_load;

drop external table staging.rtb_datas_hd_ext_:batchid;


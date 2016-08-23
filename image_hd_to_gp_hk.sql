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
  countryshort varchar(100),
  countrylong text,
  ipcity text,
  record_server bigint,
  ip_filter_flag int,
  robot_filter_flag int,
  valid_flag int,
  ht_value character varying(100),
  interest varchar(5000),
  age varchar(500),
  gender varchar(500),
  bsf varchar(500),
  tdsid varchar(500),
  tagid  varchar(500) --modified at 2016-03-04
)
location (:hdfsloc)
format 'text' (null as '' escape as 'OFF' FILL MISSING FIELDS)
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
    countryshort character varying(100),
    ht_score integer,
    interest varchar(500),
    age varchar(500),
    gender varchar(500),
    bsf smallint,
    tdsid varchar(500),
    tagid  varchar(500), --modified at 2016-03-04
    fraud  integer          --modified at 2016-03-04
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
  ht_score,
  interest,
  age,
  gender,
  bsf,
  tdsid,
  tagid,  --modified at 2016-03-04
  fraud   --modified at 2016-03-04
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
  substr(countryshort,1,2),
  case when split_part(ht_value,',',2) is null or  split_part(ht_value,',',2) = '' then 0
       when split_part(ht_value,',',2)  ~ '^[0-9]+$' then split_part(ht_value,',',2)::integer
       else 101 end,
  substr(interest,1,500),
  substr(age,1,10),
  substr(gender,1,1),
  (case when bsf in ('0','1') then bsf::smallint else null end)::smallint as bsf,
  substr(tdsid,1,100),
  substr(tagid,1,500),  --modified at 2016-03-04
  valid_flag as fraud  --modified at 2016-03-04

from staging.imageviews_hd_ext_:batchid
where valid_flag in (1,-32);

insert into xmo_dw.imageviews
   (
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
  ht_score,
  interest,
  age,
  gender,
  bsf,
  tdsid,
  tagid, 
  fraud  
)
select 
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
ht_score,
interest,
age,
gender,
bsf,
tdsid,
tagid,
fraud
from imageviews_hd_load;

drop external table staging.imageviews_hd_ext_:batchid;


\set ON_ERROR_STOP 1

set statement_timeout = 300000;

drop external table if exists staging.clickdatas_hd_ext_:batchid;
create external table staging.clickdatas_hd_ext_:batchid (
  id numeric(39,0),
  click_date timestamp without time zone,
  click_referring_site text,
  click_hash_id text,
  click_opxpid text,
  click_opxsid text,
  searchengine_id int,
  c_date_raw int,
  adtext_id text,
  adgroup_id text,
  placement text,
  leading_keyword text,
  leading_site text,
  ismobile int,
  keyword_key text,
  client_id int,
  useragent text,
  ip bigint,
  countryshort varchar(2),
  countrylong text,
  ipcity text,
  click_machine bigint,
  ip_filter_flag int,
  robot_filter_flag int,
  valid_flag int,
  ht_score int
)
location (:hdfsloc)
format 'text' (null '' escape 'OFF' fill missing fields)
log errors into staging.hd_ext_load_errors segment reject limit 10 rows;

create temporary table clickdatas_hd_load (
    id numeric(39,0) NOT NULL,
    click_date timestamp without time zone NOT NULL,
    click_slot character varying(4),
    click_machine character varying(16),
    click_referring_site text,
    click_campaign_id integer,
    click_hash_id text, -- Allow for long string to get better lookups
    click_impression smallint,
    click_number smallint,
    click_opxpid character varying(50),
    click_opxsid character varying(50),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone,
    searchengine_id integer NOT NULL,
    ip character varying(16),
    c_date integer NOT NULL,
    adtext_id bigint,
    adgroup_id bigint,
    placement character varying(500),
    countryshort character varying(2),
    countrylong character varying(44),
    ipregion character varying(36),
    ipcity character varying(44),
    ipdomain character varying(68),
    geoinfo_updated smallint,
    leading_keyword character varying(200),
    leading_site character varying(100),
    ismobile smallint,
    keyword_key text,
    dw_loaded_at timestamp without time zone DEFAULT now(),
    useragent character varying(500),
    ht_score int
) DISTRIBUTED BY (click_opxpid);

insert into clickdatas_hd_load (
id,
click_date,
click_machine,
click_referring_site,
click_hash_id,
click_opxpid,
click_opxsid,
created_at,
updated_at,
searchengine_id,
ip,
c_date,
adtext_id,
adgroup_id,
placement,
countryshort,
countrylong,
ipcity,
leading_keyword,
leading_site,
ismobile,
keyword_key,
useragent,
ht_score
)
select
id,
click_date,
host('0.0.0.0'::inet + click_machine) click_machine,
click_referring_site,
click_hash_id,
substr(click_opxpid,1,50),
substr(click_opxsid,1,50),
current_timestamp at time zone 'utc' created_at,
current_timestamp at time zone 'utc' updated_at,
searchengine_id,
host('0.0.0.0'::inet + ip) ip,
c_date_raw,
case when adtext_id is null or adtext_id = '' then null else adtext_id::bigint end,
case when adgroup_id is null or adgroup_id = '' then null else adgroup_id::bigint end,
substr(placement,1,500),
substr(countryshort,1,2),
substr(countrylong,1,44),
substr(ipcity,1,44),
substr(leading_keyword,1,100),
substr(leading_site,1,100),
ismobile,
keyword_key,
substr(useragent,1,500),
ht_score
from staging.clickdatas_hd_ext_:batchid
where valid_flag = 1;

create temporary table clickdatas_hd_load_kwid
as
select *
from (
  select a.id, a.click_opxpid, a.click_hash_id, b.hash_id, row_number() over (partition by a.id order by b.updated_at desc) rn
  from clickdatas_hd_load a join xmo_dw.keywords b
  on a.searchengine_id = b.searchengine_id
  and a.keyword_key = b.keyword_id
  where a.click_hash_id like 'kwid:%'
) m
where rn = 1
distributed by (click_opxpid);

create temporary table clickdatas_hd_load_kw
as
select *
from (
  select a.id, a.click_opxpid, a.click_hash_id, b.hash_id, a.adgroup_id || '|' || b.keyword_id keyword_key, 
    row_number() over (partition by a.id 
      order by 
        b.status_i desc, 
        b.status_s, 
        b.created_at desc,
        case when lower(regexp_replace(click_hash_id, E'^kw:\\S:', '')) = lower(keyword) then 0 else 1 end) rn
  from clickdatas_hd_load a join xmo_dw.keywords b
  on a.searchengine_id = b.searchengine_id
  and a.adgroup_id = b.unitid
  and (
    case split_part(a.click_hash_id, ':', 2)
      when 'e' then 'exact'
      when 'b' then 'broad'
      when 'p' then 'phrase'
      else null
    end
  ) = lower(b.matchtype)
  and (
    lower(regexp_replace(click_hash_id, E'^kw:\\S:', '')) = lower(keyword)
    or
    lower(regexp_replace(click_hash_id, E'^kw:\\S:', '')) = lower(replace(keyword, ' ', ''))
  )
  where a.click_hash_id like 'kw:%'
) m
where rn = 1
distributed by (click_opxpid);

update clickdatas_hd_load a
set click_hash_id = b.hash_id
from clickdatas_hd_load_kwid b
where a.id = b.id
and a.click_opxpid = b.click_opxpid;

update clickdatas_hd_load a
set 
  click_hash_id = b.hash_id,
  keyword_key = b.keyword_key
from clickdatas_hd_load_kw b
where a.id = b.id
and a.click_opxpid = b.click_opxpid;

insert into xmo_dw.clickdatas (
id,
click_date,
click_slot,
click_machine,
click_referring_site,
click_campaign_id,
click_hash_id,
click_impression,
click_number,
click_opxpid,
click_opxsid,
created_at,
updated_at,
searchengine_id,
ip,
c_date,
adtext_id,
adgroup_id,
placement,
countryshort,
countrylong,
ipcity,
leading_keyword,
leading_site,
ismobile,
keyword_key,
dw_loaded_at,
useragent,
ht_score
)
select
id,
click_date,
-1 click_slot,
click_machine,
click_referring_site,
-1 click_campaign_id,
substr(click_hash_id,1,36),
0 click_impression,
0 click_number,
click_opxpid,
click_opxsid,
created_at,
updated_at,
searchengine_id,
ip,
c_date,
adtext_id,
adgroup_id,
placement,
countryshort,
countrylong,
ipcity,
leading_keyword,
leading_site,
ismobile,
substr(keyword_key,1,41),
dw_loaded_at,
useragent,
ht_score
from clickdatas_hd_load;

drop external table staging.clickdatas_hd_ext_:batchid;


\set ON_ERROR_STOP 1

set statement_timeout = 300000;

drop external table if exists staging.adclicks_hd_ext_:batchid;
create external table staging.adclicks_hd_ext_:batchid (
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
  ismobile text,
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
  bid_request_id varchar(64),
  aud_hash text,
  crm_hash text,
  adslot_id smallint,
  canonical text,
  domain text,
  device text,
  ht_value character varying(100),
  tagid  character varying(500),
  adx_name  character varying(20) --modified at 2016-03-29
)
location (:hdfsloc)
format 'text' (null '' escape 'OFF' fill missing fields)
log errors into staging.hd_ext_load_errors segment reject limit 100 rows;


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
    bid_request_id varchar(64),
    aud_hash varchar(32),
    crm_hash varchar(32),
    adslot_id smallint,
    canonical character varying(60),
    domain character varying(100),
    device character varying(44),
    ht_score int,
    tagid  character varying(500),  --modified at 2016-03-04
    fraud integer,
    adx_name character varying(20) --modified at 2016-03-29
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
bid_request_id,
aud_hash,
crm_hash,
adslot_id,
canonical,
domain,
device,
ht_score,
tagid, --modified at 2016-03-04
fraud,
adx_name
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
case
  when ismobile is null or ismobile = '' then null
  when ismobile ~ '^[0-9]+$' then 1
  else null
end,
keyword_key,
substr(useragent,1,500),
bid_request_id,
substr(aud_hash,1,32),
substr(crm_hash,1,32),
adslot_id,
substr(canonical, 1, 60),
substr(domain, 1, 100),
substr(device, 1, 44),
case when split_part(ht_value,',',2) is null or  split_part(ht_value,',',2) = '' then 101
     when split_part(ht_value,',',2)  ~ '^[0-9]+$' then split_part(ht_value,',',2)::integer
     else 101 end,
substr(tagid,1,500), --modified at 2016-03-04
valid_flag as fraud,
substr(adx_name, 1, 20) --modified at 2016-03-29
from staging.adclicks_hd_ext_:batchid
where valid_flag in(1,-32);

create temporary table clickdatas_hd_load_adgid
as
select *
from (
  select a.id, a.click_opxpid, b.adgroup_id, row_number() over (partition by a.id order by b.status_i desc, b.created_at desc) rn
  from clickdatas_hd_load a join xmo_dw.adtexts b
  on a.searchengine_id = b.searchengine_id
  and a.adtext_id = b.adtext_id
  where a.adtext_id is not null
  and a.adgroup_id is null
) m
where rn = 1
distributed by (click_opxpid);

update clickdatas_hd_load a
set adgroup_id = b.adgroup_id
from clickdatas_hd_load_adgid b
where a.id = b.id
and a.click_opxpid = b.click_opxpid;

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
bid_request_id,
aud_hash,
crm_hash,
adslot_id,
canonical,
domain,
device,
ht_score,
tagid,--modified at 2016-03-04
fraud,
adx_name --modified at 2016-03-04
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
bid_request_id,
aud_hash,
crm_hash,
adslot_id,
canonical,
domain,
device,
ht_score,
tagid,--modified at 2016-03-04
fraud,
adx_name --modified at 2016-03-29
from clickdatas_hd_load;

drop external table staging.adclicks_hd_ext_:batchid;


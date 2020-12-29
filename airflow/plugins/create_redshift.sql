CREATE TABLE IF NOT EXISTS public.matches (
    match_id VARCHAR(256),
    game_size INTEGER,
    match_mode VARCHAR(64),
    map VARCHAR(64) )
DISTSTYLE all
SORTKEY (match_id) ;

CREATE TABLE IF NOT EXISTS public.details (
    detail_id VARCHAR(64),
    match_id VARCHAR(256),
    killer_name VARCHAR(64),
    killer_placement INTEGER,
    victim_name VARCHAR(64),
    victim_placement INTEGER,
    weapon VARCHAR(64),
    time INTEGER )
SORTKEY (detail_id) ;

CREATE TABLE IF NOT EXISTS public.dates (
    game_time DATETIME,
    match_id VARCHAR(256),
    year INTEGER,
    month INTEGER,
    hour INTEGER,
    week INTEGER,
    weekday INTEGER )
SORTKEY (game_time) ;

CREATE TABLE IF NOT EXISTS public.performances (
    performance_id VARCHAR(128),
    name VARCHAR(64),
    match_id VARCHAR(256),
    dbno INTEGER,
    dist_ride FLOAT,
    dist_walk FLOAT,
    dmg INTEGER,
    kills INTEGER,
    survive_time FLOAT,
    assists INTEGER )
SORTKEY (performance_id) ;

CREATE TABLE IF NOT EXISTS public.locations (
    location_id BIGINT,
    killer_position_x FLOAT,
    killer_position_y FLOAT,
    victim_position_x FLOAT,
    victim_position_y FLOAT,
    match_id VARCHAR(256),
    detail_id VARCHAR(64),
    date_id DATETIME,
    performance_id VARCHAR(128) )
DISTSTYLE key
DISTKEY (location_id)
COMPOUND SORTKEY (killer_position_x, killer_position_y) ;

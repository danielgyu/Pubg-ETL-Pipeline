## dimension table - match
column | type | description | sample
--- | --- | --- | ---
match_id | string | primary key | 2U4GBNA0Ymk0...
game_size | integer | number of participants | 48
match_mode | string | different game modes | tpp 

## dimension table - dates
column | type | description | sample
--- | --- | --- | ---
game_time | datetime | datetime of the match | 2018-01-06 22:54:56
match_id | string | a distinct value for the match | 2U4GBNA0Ymk0...
year | integer | year data of the match | 2018
month | integer | month data of the match | 10
hour | integer | hour data of the match | 13
week | integer | week number of the match | 20
weekday | string | weekday data of the match | 1 (for mondday)

## dimension table - details
column | type | description | sample
--- | --- | --- | ---
detail_id | integer | primary key | 2U4GB1753Maroccockblock 
match_id | string | a distinct value of the match | 2U4GBNA0Ymk0...
killer_name | string | killer name of the event | Maroccockblock
killer_placement | integer | final position of the player | 3
victim_name | string | victim name of the event | LIONAAAA
victim_placement | integer | final position of the victim | 5
weapon | string | weapon name of the event | M16A4
time | integer | time data of the event (in seconds) | 1753

## dimension table - performances
column | type | description | sample
--- | --- | --- | ---
performance_id | stirng | primary key | 2U4GB-SSniubi
match_id | string | a distinct value of the match | 2U4GBNA0Ymk0...
name | string | game name of the player | Maroccockblock
dmg | integer | total damage dealt in the game | 277
kills | integer | total kills in the game | 3
dbno | integer | number of dbno(Down But Not Out) in the game | 1
assist | integer | totalkill assists in the game | 0
survive_time | float | survive time in the game in seconds | 250.414
dist_ride | float | total amount of distance travelled with vehicles | 12.0
dist_walk | float | total amount of distance travelled by feet | 184.571228

## fact table = location
column | type | description | sample
--- | --- | --- | ---
location_id integer | primary key | 8589962945
killer_position_x | float | x-axis location of killer (0.0-800000.0) | 0.0
killer_position_y | float | y-axis location of killer | 500000.0
victim_position_x | float | x-axis location of victim | 511080.6875
victim_position_y | float | y-axis location of victim | 614471.3125
match_id | string | match table foreign key | 2U4GBNA0Ymk0...
detail_id | string | detail table foreign key | 2U4GB1753Maroccockblock 
date_id | datetime | date table foreign key | 2018-01-06 22:54:56
performance_id | string | performance table foreign key | 2U4GB-SSniubi

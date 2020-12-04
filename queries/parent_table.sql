CREATE TABLE IF NOT EXISTS users (
id integer PRIMARY KEY,
name varchar(120) NOT NULL,
age integer NOT NULL,
gender varchar(10),
city varchar(120),
primary_activity varchar(120),
status varchar(20),
registration date,
last_active date,
tmp1 integer,
tmp2 bool
)

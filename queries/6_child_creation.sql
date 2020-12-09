CREATE TABLE IF NOT EXISTS userattributes (
id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
user_id integer NOT NULL,
FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE,
attribute_id integer NOT NULL,
FOREIGN KEY (attribute_id) REFERENCES attributes(id) ON DELETE CASCADE ON UPDATE CASCADE,
answer varchar(120) NOT NULL,
updated_at timestamp NOT NULL,
UNIQUE(user_id, attribute_id)
)

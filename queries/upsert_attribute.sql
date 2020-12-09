INSERT INTO userattributes
VALUES (default, $1, $2, 'n', NOW())
ON CONFLICT ON CONSTRAINT userattributes_user_id_attribute_id_key
DO UPDATE SET answer='u' 

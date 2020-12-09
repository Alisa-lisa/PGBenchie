SELECT u.name as contact, a.name as attribute 
FROM users u, attributes a, userattributes ua
WHERE a.id = ua.attribute_id
AND u.id = ua.user_id
AND ua.updated_at + concat(a.expiry,' days')::interval <= NOW()

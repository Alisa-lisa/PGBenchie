SELECT ua.id 
FROM userattributes ua, attributes a
WHERE ua.attribute_id = a.id
AND a.name= $1
AND ua.updated_at + concat(a.expiry, ' days')::interval > now()
LIMIT $2

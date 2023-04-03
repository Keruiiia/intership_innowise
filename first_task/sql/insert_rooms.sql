INSERT INTO rooms (id, name) VALUES (%s, %s)
ON CONFLICT (id) DO UPDATE SET name = excluded.name;
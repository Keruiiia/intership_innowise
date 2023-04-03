SELECT rooms.name, AVG(EXTRACT(YEAR FROM age(now(), students.birthday))) as avg_age
FROM rooms
LEFT JOIN students ON rooms.id = students.room_id
GROUP BY rooms.id
ORDER BY avg_age ASC
LIMIT 5;
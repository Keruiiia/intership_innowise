SELECT rooms.name, (EXTRACT(YEAR FROM age(MAX(students.birthday), MIN(students.birthday)))) as age_difference
FROM rooms
LEFT JOIN students ON rooms.id = students.room_id
GROUP BY rooms.id
ORDER BY age_difference DESC
LIMIT 5;
SELECT rooms.name, COUNT(students.id) as num_students
FROM rooms
LEFT JOIN students ON rooms.id = students.room_id
GROUP BY rooms.id;
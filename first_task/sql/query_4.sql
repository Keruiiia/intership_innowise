SELECT r.name
FROM rooms r
INNER JOIN students s ON s.room_id = r.id
INNER JOIN (
    SELECT DISTINCT room_id, sex
    FROM students
) s1 ON s.room_id = s1.room_id AND s.sex <> s1.sex
GROUP BY r.name
HAVING COUNT(*) > 1;


INSERT INTO students (birthday, id, name, room_id, sex)
                VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    name = excluded.name,
    birthday = excluded.birthday,
    room_id = excluded.room_id,
    sex = excluded.sex;

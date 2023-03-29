'''Командный интерфейс должен поддерживать следующие входные параметры

students (путь к файлу студентов)

rooms (путь к файлу комнат)

format (выходной формат: xml или json)

использовать ООП и SOLID.

отсутствие использования ORM (использовать SQL)

С использованием базы MySQL (или другая реляционная БД, например, PostgreSQL) создать схему данных соответствующую файлам во вложении (связь многие к одному)

Написать скрипт, целью которого будет загрузка этих двух файлов и запись данных в базу'''

import argparse

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="read file, get argument"
    )
    parser.add_argument(
        "-s", "--students"
    )
    parser.add_argument(
        "-r", "--rooms"
    )
    parser.add_argument(
        "-f", "--format"
    )
    return parser

parser = init_argparse()
args = parser.parse_args()
print(args.students)
print(args.rooms)
print(args.format)


import json

rooms_file = open(args.rooms)
rooms_data = json.load(rooms_file)
for i, room in enumerate(rooms_data):
    rooms_data[i] = tuple(rooms_data[i].values())
rooms_file.close()

students_file = open(args.students)
students_data = json.load(students_file)
for i, student in enumerate(students_data):
    students_data[i] = tuple(students_data[i].values())
students_file.close()

import psycopg2

connection = psycopg2.connect(
                            database="rooms_and_students",
                            user="postgres",
                            password="postgres",
                            host="localhost",
                            port=5432
)

cur = connection.cursor()

cur.executemany("INSERT INTO rooms (id, name) VALUES (%s, %s)", rooms_data)
cur.executemany("INSERT INTO students (birthday, id, name, room_id, sex) \
                VALUES (%s, %s, %s, %s, %s)", students_data)
cur.execute("SELECT name FROM students LIMIT 10")
rows = cur.fetchall()
for row in rows:
    print(row)



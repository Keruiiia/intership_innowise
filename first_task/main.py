'''Командный интерфейс должен поддерживать следующие входные параметры

students (путь к файлу студентов)

rooms (путь к файлу комнат)

format (выходной формат: xml или json)

использовать ООП и SOLID.

отсутствие использования ORM (использовать SQL)

С использованием базы MySQL (или другая реляционная БД, например, PostgreSQL) создать схему данных соответствующую файлам во вложении (связь многие к одному)

Написать скрипт, целью которого будет загрузка этих двух файлов и запись данных в базу'''

import argparse
import os
import psycopg2
import json
from dotenv import load_dotenv


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


def get_sql_query(name: str) -> str:
    query_file_name = os.path.join("sql", name)
    file = open(query_file_name, "r")
    content = file.read()
    file.close()
    return content


load_dotenv()

parser = init_argparse()
args = parser.parse_args()
print(args.students)
print(args.rooms)
print(args.format)

# json

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


connection = psycopg2.connect(
    database=os.getenv("POSTGRES_DATABASE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

cur = connection.cursor()

cur.executemany(get_sql_query('insert_rooms.sql'), rooms_data)
cur.executemany(get_sql_query('insert_students.sql'), students_data)
connection.commit()
cur.execute("SELECT name FROM students LIMIT 10")
rows = cur.fetchall()
for row in rows:
    print(row)


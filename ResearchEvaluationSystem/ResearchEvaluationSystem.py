import psycopg
from psycopg import OperationalError
from psycopg.types.json import Jsonb
from psycopg.rows import dict_row
from dotenv import load_dotenv
import os
import json

load_dotenv()


class EvaluationSystemApi:
    def __init__(self, port=5432):
        self.password = None
        self.login = None
        self.baza = None
        self.host = None
        self.port = port
        self.conn = None
        self.API_CALLS = {"open": self.connect,
                          "article": self.article,
                          "points": self.points,
                          "institution": self.institution,
                          "author": self.author,
                          "author_details": self.author_details,
                          "newuser": self.new_user,
                          "deluser": self.del_user}

    def connect(self, args):
        self.host = args["host"]
        self.baza = args["baza"]
        self.login = args["login"]
        self.password = args["password"]
        try:
            self.conn = psycopg.connect(
                host=self.host,
                dbname=self.baza,
                user=self.login,
                password=self.password,
                port=5432,
                row_factory=dict_row
            )
            self.conn.adapters.register_loader("numeric",
                                               psycopg.types.numeric.FloatLoader)
            return {"status": "OK"}
        except OperationalError:
            return {"status": "ERROR"}

    def article(self, args):
        result = {"status": "OK"}
        if self.conn is None:
            return {"status": "ERROR"}

        with self.conn.cursor() as cur:
            cur.execute("CALL new_article(%s)",
                        [Jsonb(args)])
        cur.close()
        return result

    def points(self, args):
        result = {"status": "OK"}
        if self.conn is None:
            return {"status": "ERROR"}
        with self.conn.cursor() as cur:
            cur.execute("CALL new_conference_points(%s)",
                        [Jsonb(args)])
        return result

    def institution(self, args):
        result = {"status": "OK"}
        if self.conn is None:
            return {"status": "ERROR"}

        with self.conn.cursor() as cur:
            result["data"] = cur.execute(
                "SELECT * FROM get_institution_points(%s)",
                [Jsonb(args)]
            ).fetchall()
        cur.close()
        return result

    def author(self, args):
        result = {"status": "OK"}
        if self.conn is None:
            return {"status": "ERROR"}

        with self.conn.cursor() as cur:
            result["data"] = cur.execute(
                "SELECT * FROM get_author_points(%s)",
                [Jsonb(args)]
            ).fetchall()
        cur.close()
        return result

    def author_details(self, args):
        result = {"status": "OK"}

        if self.conn is None:
            return {"status": "ERROR"}

        with self.conn.cursor() as cur:
            result["data"] = cur.execute(
                "SELECT * FROM get_author_details(%s)",
                [Jsonb(args)]
            ).fetchall()
        cur.close()
        return result

    def new_user(self, args):
        result = {"status": "OK"}
        if self.conn is None or args.get("secret") != os.getenv("SECRET"):
            return {"status": "ERROR"}

        with self.conn.cursor() as cur:
            cur.execute("CALL new_user(%s)",
                        [Jsonb(args)])
        cur.close()
        return result

    def del_user(self, args):
        result = {"status": "OK"}
        if self.conn is None or args.get("secret") != os.getenv('SECRET'):
            return {"status": "ERROR"}

        with self.conn.cursor() as cur:
            result["status"] = cur.execute("SELECT del_user(%s)",
                        [Jsonb(args)]).fetchone()["del_user"]
        cur.close()
        return result

    def authorization(self, args, func):
        if self.conn is None:
            return {"status": "ERROR"}

        with self.conn.cursor() as cur:
            res = cur.execute("SELECT check_pwd(%s)",
                        [Jsonb(args)]).fetchone()["check_pwd"]
            if res:
                func()

    def execute(self, filepath, output_path="out.jsonl"):
        out_file = open(output_path, 'w+')
        with open(filepath, 'r') as file:
            for line in file:
                func = json.loads(line)
                f = next(iter(func))
                if f in ["article", "points"]:
                    output = self.authorization(self.API_CALLS[f](func[f]), f)
                else:
                    output = self.API_CALLS[f](func[f])
                out_file.write(json.dumps(output))
                out_file.write('\n')
        out_file.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()




e = EvaluationSystemApi()
e.execute("/home/fabian/Desktop/4 sem/bd/projekt/BD projekt/input.jsonl")



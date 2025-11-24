import os
from sqlalchemy import create_engine



class Connection:
    connection_string = os.environ["DATABASE_URI"]
    engine = create_engine(url=connection_string)


db_connection = Connection()

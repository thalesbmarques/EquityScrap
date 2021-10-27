import pandas as pd
import sqlite3

# Creating connection
conn = sqlite3.connect('db/data.db3')
movida = pd.read_sql('SELECT * FROM movida', con=conn)
conn.close()

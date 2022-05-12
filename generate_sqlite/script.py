import sqlite3
from countries import countries
import random

con = sqlite3.connect('companies.db')
cur = con.cursor()

test_countries = ['chad', 'myanmar', 'suriname', 'thailand']
#(name, domain, year_founded_industry, size range, locality, country, current_employees, total_employe)


n = 10000000

for i in range(n):
    if i in [69, 420, 3829, 19348, 390239, 794832, 2000345, 5901392]:
        country = random.choice(test_countries)
    else:
        country = random.choice(countries)

    cur.execute('insert into companies (name, domain, year_founded, industry, "size range", locality, country, current_employees, total_employees) values (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            ['blah blah blah', 'blah blah', '2003', 'lol', 'bah', 'lol392 blah bahl', country.lower(), 393, 930])

con.commit()



cur.close()
con.close()

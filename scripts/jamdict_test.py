from jamdict import Jamdict
from pathlib import Path

jam = Jamdict()

# Access metadata through Peewee model
# for row in jam.jmdict.meta.select():
#     print(row.key, row.value)

print(Path.home())
print(jam.lookup('戦い方'))
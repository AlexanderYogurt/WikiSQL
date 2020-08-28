import pdb
import jsonlines

# from lib.table import Table
# from lib.dbengine import DBEngine
# db = DBEngine("/home/ec2-user/efs/nel_data/WikiSQL/data/dev.db").conn
# table = Table.from_db(db, "1-10015132-11")


# ==============================================================================
# e.g. SELECT Position FROM table_1-10015132-11 WHERE School/Club Team = "Butler CC (KS)"
# e.g. SELECT COUNT(Position) FROM table_1-10015132-9 WHERE Years in Toronto = \"2006-07\"

agg_ops = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
cond_ops = ['=', '>', '<', 'OP']

with jsonlines.open("/home/ec2-user/efs/nel_data/WikiSQL/data/dev.tables.jsonl") as f:
    tables = [l for l in f.iter()]
tables = {x["id"]: x for x in tables}

table = tables["1-10015132-11"]
header = [x.lower() for x in table["header"]]


def parse_sql_str(sql):

    parsed_sql = {"phase": 1, "query": {}}

    # from table
    from_table = re.search(r'FROM(.*?)WHERE', sql).group(1).strip().lower() # 'table_1-10015132-11'
    try:
        table_id = from_table.replace("table_", "") # '1-10015132-11'
        table = tables[table_id]
        header = [x.lower() for x in table["header"]]
        parsed_sql["table_id"] = table_id
    except:
        parsed_sql["error"] = "Generated SQL is not valid"
        return False, parsed_sql

    # select column
    sel_col = re.search(r'SELECT(.*?)FROM', sql).group(1).strip() # COUNT(Position)
    for idx, op in enumerate(agg_ops):
        if op in sel_col and op != '':
            sel_col = re.search(f'{op}\((.*?)\)', sel_col).group(1).strip()
            break

    try:
        sel = header.index(sel_col.lower()) # 3
        parsed_sql["query"]["sel"] = sel
        parsed_sql["query"]["agg"] = idx
    except:
        parsed_sql["error"] = "Generated SQL is not valid"
        return False, parsed_sql

    # where clause
    where = re.search(r'(?<=WHERE).*$', sql).group(0).strip() # 'Position = "Guard" AND Years in Toronto = "1996-97"'
    wheres = where.split("AND")
    conds = []
    for cond in wheres:
        cond = cond.strip() # 'Position = "Guard"'
        for idx, op in enumerate(cond_ops):
            if op in cond:
                col, value = cond.split(op)
                col = col.strip()
                value = value.strip()
                try:
                    sel = header.index(col.lower())
                    conds.append([sel, idx, value])
                except:
                    pass
                break

    parsed_sql["query"]["conds"] = conds # can be empty list
    parsed_sql["error"] = ""

    return True, parsed_sql

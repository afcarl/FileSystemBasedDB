import re, os, itertools
import sqlparse

# {table_name: ["column1"...], ...}
all_tables = {}

# -----------------------------------------------------------------------------
def avg(l):
    if l == []:
        return -1
    return sum(l) * 1.0 / len(l)

# -----------------------------------------------------------------------------
def distinct(l):
    unique_list = []
    for i in l:
        if i not in unique_list:
            unique_list.append(i)
            print i
    return

# -----------------------------------------------------------------------------
def remove_unnecessary(x):
    if x is None:
        return False
    x = x.strip()
    return x != "" and x != "<begin_table>" and x != "<end_table>"

# -----------------------------------------------------------------------------
def print_table(columns, records):

    header = ", ".join(columns)

    print "-" * len(header)
    print header
    print "-" * len(header)

    for record in records:
        print ", ".join(map(lambda x: str(x), record))

# -----------------------------------------------------------------------------
def create_table(query):
    res = sqlparse.parse(query)[0]
    try:
        tmp = re.match("CREATE TABLE \w+\((\w+ \w+,? ?)+\)\;", query).group()
    except AttributeError:
        print "CREATE TABLE: Invalid syntax"
        return

    table_func = res.tokens[-2]
    table_name = table_func.tokens[0].value

    if all_tables.has_key(table_name):
        print "CREATE TABLE: Table already exists"
        return

    params = str(table_func.tokens[1])[1:-1]
    columns = map(lambda x: x.strip().split(" ")[0], params.split(","))

    if len(set(columns)) != len(columns):
        print "CREATE TABLE: Duplicate columns provided"
        return

    # Append to metadata.txt
    f = open("metadata.txt", "a+")
    file_string = "\r\n<begin_table>\r\n" + \
                  table_name + "\r\n" + \
                  "\r\n".join(columns) + "\r\n" + \
                  "<end_table>\r\n";
    f.write(file_string)
    f.close()

    # Add table to all_tables dict
    all_tables[table_name] = columns
    # Create table_name.csv
    os.system("touch " + table_name + ".csv")

# -----------------------------------------------------------------------------
def insert_table(query):
    res = sqlparse.parse(query)[0]
    try:
        tmp = re.match("INSERT INTO \w+ VALUES ?\((\-?\d+((\, )|\,)?)+\)\;",
                       query)
    except AttributeError:
        print "INSERT INTO: Invalid Syntax"
        return

    table_name = str(res.tokens[4])
    if all_tables.has_key(table_name) is False:
        print "INSERT INTO: Table does not exist"
        return

    params = str(res.tokens[-2])[1:-1]
    values = map(lambda x: x.strip(), params.split(","))

    if len(all_tables[table_name]) != len(values):
        print "INSERT INTO: Table %s expects %d columns, %d given" \
                            % (table_name,
                               len(all_tables[table_name]),
                               len(values))
        return

    f = open(table_name + ".csv", "a+")
    f.write(",".join(values) + "\r\n")
    f.close()

# -----------------------------------------------------------------------------
def truncate_table(query):

    try:
        tmp = re.match("TRUNCATE TABLE \w+\;", query).group()
    except AttributeError:
        print "TRUNCATE TABLE: Invalid Syntax"
        return

    table_name = query.split(" ")[-1].strip(";")
    if all_tables.has_key(table_name) is False:
        print "TRUNCATE TABLE: Table does not exist"
        return

    os.system("rm " + table_name + ".csv")
    os.system("touch " + table_name + ".csv")

# -----------------------------------------------------------------------------
def drop_table(query):
    try:
        tmp = re.match("DROP TABLE \w+\;", query).group()
    except AttributeError:
        print "DROP TABLE: Invalid Syntax"
        return

    table_name = query.split(" ")[-1].strip(";")
    if all_tables.has_key(table_name) is False:
        print "DROP TABLE: Table does not exist"
        return

    f = open(table_name + ".csv")
    tmp = f.read()
    f.close()
    if tmp != "":
        print "DROP TABLE: Table contains one or more records"
    else:
        complete = ""
        # Dirty cool hacks here :p
        del all_tables[table_name]
        for table in all_tables:
            complete += "<begin_table>\r\n"
            complete += table + "\r\n"
            complete += "\r\n".join(all_tables[table]) + "\r\n"
            complete += "<end_table>\r\n"

        # Remove table from metadata.txt
        f = open("metadata.txt", "w")
        f.write(complete)
        f.close()

        # @ToDo: check if file exists
        # Remove table_name.csv
        os.system("rm " + table_name + ".csv")

# -----------------------------------------------------------------------------
def delete_record(query):

    try:
        tmp = re.match("DELETE FROM \w+ WHERE .*;", query).group()
    except AttributeError:
        print "DELETE FROM: Invalid Syntax"
        return

    query = query.split()
    table_name = query[2]

    if all_tables.has_key(table_name) is False:
        print "DELETE FROM: Table does not exist"
        return

    where_condition = " ".join(query[4:])[:-1]
    res = sqlparse.parse(where_condition)[0]
    columns_used = get_columns(res)

    if set(columns_used).issubset(set(all_tables[table_name])) is False:
        print "DELETE FROM: Invalid column in WHERE condition"
        return

    where_condition = where_condition.replace(" AND ", " and ")
    where_condition = where_condition.replace(" OR ", " or ")
    where_condition = where_condition.replace(" = ", " == ")

    for column in xrange(len(columns_used)):
        column_index = all_tables[table_name].index(columns_used[column])
        where_condition = where_condition.replace(columns_used[column],
                                                  "this_record[%d]" % (column_index))

    f = open(table_name + ".csv")
    records = f.readlines()
    f.close()
    all_records = map(lambda x: x.strip().split(","), records)
    filtered_records = []
    for record in all_records:
        this_record = map(lambda x: int(x), record)
        if eval(where_condition) is False:
            filtered_records.append(record)
    if len(filtered_records) != 0:
        f = open(table_name + ".csv", "w")
        f.write("\r\n".join(map(lambda x: ",".join(x), filtered_records)) + "\r\n")
        f.close()
    else:
        os.system("rm " + table_name + ".csv")
        os.system("touch " + table_name + ".csv")

# -----------------------------------------------------------------------------
def get_columns(condition):

    all_identifiers = []

    try:
        t = condition.tokens
    except AttributeError:
        if type(condition) == 'sqlparse.sql.Identifier':
            return [condition.value]
        return []

    for token in condition.tokens:
        if isinstance(token, sqlparse.sql.Identifier):
            all_identifiers.append(token.value)
        else:
            all_identifiers.extend(get_columns(token))

    return all_identifiers

# -----------------------------------------------------------------------------
def get_aggregate(function_name, table_name, column_name):

    function_dict = {"SUM": sum,
                     "MIN": min,
                     "MAX": max,
                     "AVG": avg,
                     "DISTINCT": distinct}
    f = open(table_name + ".csv")
    records = f.readlines()
    f.close()
    records = map(lambda x: map(lambda y: int(y),
                                x.strip().split(",")),
                  records)

    column_index = all_tables[table_name].index(column_name)
    l = map(lambda x: x[column_index], records)

    try:
        ans = function_dict[function_name](l)
    except KeyError:
        print "Invalid function in SELECT query"
        return
    if ans:
        print ans
    return

# -----------------------------------------------------------------------------
def select_records(query):

    try:
        tmp = re.match("SELECT [A-Z]+\(\w+\) FROM \w+\;", query).group()
        tokens = sqlparse.parse(tmp)[0].tokens
        if isinstance(tokens[2], sqlparse.sql.Function) is False:
            print "SELECT: Invalid syntax for aggregate functions"
            return
        function_name = tokens[2].tokens[0].value
        table_name = tokens[-2].value
        if all_tables.has_key(table_name) is False:
            print "SELECT: Table %s does not exist" % table_name
            return
        try:
            column_name = tokens[2].tokens[1].value[1:-1]
            index = all_tables[table_name].index(column_name)
        except ValueError:
            print "SELECT: Table %s have no such column %s" \
                            % (table_name, column_name)
            return

        get_aggregate(function_name, table_name, column_name)
        return
    except AttributeError:
        pass

    components = re.split("SELECT|FROM|WHERE", query)[1:]

    # Columns to display in the results
    columns = map(lambda x: x.strip(), components[0].split(","))
    table_names = map(lambda x: x.strip(), components[1].strip(";").split(","))
    valid_columns = []
    for table in table_names:
        valid_columns.extend(all_tables[table])

    if len(set(valid_columns)) != len(valid_columns):
        print "SELECT: Atleast two of the tables have same column names"
        return

    # Select all columns
    if columns[0] == "*":
        columns = []
        for table in table_names:
            columns.extend(all_tables[table])

        if len(set(columns)) != len(columns):
            print "SELECT: Atleast two of the tables have same column names"
            return

    combined_records = []
    for table in table_names:
        f = open(table + ".csv")
        records = f.readlines()
        records = map(lambda x: map(lambda y: int(y),
                                    x.strip().split(",")),
                      records)
        combined_records.append(records)
        f.close()

    try:
        where_condition = components[2].strip(";")
    except IndexError:
        # Select all
        where_condition = "1"

    res = sqlparse.parse(where_condition)[0]
    # Columns used in where condition
    columns_used = get_columns(res)

    if set(columns_used).issubset(set(valid_columns)) is False or \
       set(columns).issubset(set(valid_columns)) is False:
        print "SELECT: Invalid column name in query"
        return

    # Index of column in its table
    table_indices = []
    for column in columns_used:
        for table in all_tables:
            try:
                tmp_index = all_tables[table].index(column)
                table_indices.append((table, tmp_index))
            except ValueError:
                continue

    where_condition = where_condition.replace(" AND ", " and ")
    where_condition = where_condition.replace(" OR ", " or ")
    where_condition = where_condition.replace(" = ", " == ")

    for column in xrange(len(columns_used)):
        table_index = "table_indices[%d]" % (column)
        replace_with = "element[table_names.index(%s[0])][%s[1]]" \
                                % (table_index, table_index)
        where_condition = where_condition.replace(columns_used[column],
                                                  replace_with)

    display_list = []
    for column in xrange(len(columns)):
        for table in table_names:
            try:
                tmp_index = all_tables[table].index(columns[column])
                display_list.append("element[%d][%d]" % \
                                        (table_names.index(table),
                                         tmp_index))
            except ValueError:
                continue

    for element in itertools.product(*combined_records):
        if eval(where_condition):
            for column in display_list:
                print eval(column),
            print

# -----------------------------------------------------------------------------
def parse_query(query):

    query_type = query.split()[0]
    if query_type == "CREATE":
        create_table(query)
    elif query_type == "INSERT":
        insert_table(query)
    elif query_type == "TRUNCATE":
        truncate_table(query)
    elif query_type == "DROP":
        drop_table(query)
    elif query_type == "DELETE":
        delete_record(query)
    elif query_type == "SELECT":
        select_records(query)
    else:
        return 0
    return 1

# -----------------------------------------------------------------------------
if __name__ == "__main__":

    f = open("metadata.txt")
    meta_data = f.read()
    f.close()
    meta_data = re.split("(<begin_table>)|(<end_table>)", meta_data)
    tables = filter(remove_unnecessary, meta_data)
    tables = map(lambda x: x.split("\r\n"), tables)
    tables = map(lambda x: filter(remove_unnecessary, x), tables)

    for table in tables:
        all_tables[table[0]] = table[1:]

    while True:
        query = raw_input("sql> ")
        res = parse_query(query)
        if res == 0:
            break

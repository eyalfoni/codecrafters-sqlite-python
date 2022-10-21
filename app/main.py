import sys

from dataclasses import dataclass

# import sqlparse - available if you need it!

from .record_parser import parse_record
from .varint_parser import parse_varint

database_file_path = sys.argv[1]
command = sys.argv[2]


@dataclass(init=False)
class PageHeader:
    page_type: int
    first_free_block_start: int
    number_of_cells: int
    start_of_content_area: int
    fragmented_free_bytes: int
    right_most_pointer: int

    @classmethod
    def parse_from(cls, database_file):
        """
        Parses a page header as mentioned here: https://www.sqlite.org/fileformat2.html#b_tree_pages
        """
        instance = cls()

        instance.page_type = int.from_bytes(database_file.read(1), "big")
        instance.first_free_block_start = int.from_bytes(database_file.read(2), "big")
        instance.number_of_cells = int.from_bytes(database_file.read(2), "big")
        instance.start_of_content_area = int.from_bytes(database_file.read(2), "big")
        instance.fragmented_free_bytes = int.from_bytes(database_file.read(1), "big")
        if instance.page_type == 5 or instance.page_type == 2:
            instance.right_most_pointer = int.from_bytes(database_file.read(4), "big")

        return instance

    def __repr__(self):
        return f'page_type: {self.page_type}\nnumber_of_cells {self.number_of_cells}\n' \
               f'start_of_content_area {self.start_of_content_area}'


def read_sqlite_schema_rows():
    with open(database_file_path, "rb") as database_file:
        database_file.seek(100)  # Skip the header section
        page_header = PageHeader.parse_from(database_file)
        database_file.seek(100 + 8)  # Skip the database header & b-tree page header, get to the cell pointer array

        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

        sqlite_schema_rows = []

        # Each of these cells represents a row in the sqlite_schema table.
        for cell_pointer in cell_pointers:
            database_file.seek(cell_pointer)
            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)
            record = parse_record(database_file, 5)

            # Table contains columns: type, name, tbl_name, rootpage, sql
            sqlite_schema_rows.append({
                'type': record[0],
                'name': record[1],
                'tbl_name': record[2],
                'rootpage': record[3],
                'sql': record[4],
            })

        return sqlite_schema_rows


if command == ".dbinfo":
    sqlite_schema_rows = read_sqlite_schema_rows()

    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    print(f"number of tables: {len(sqlite_schema_rows)}")
elif command == ".tables":
    sqlite_schema_rows = read_sqlite_schema_rows()
    names = [entry['name'].decode() for entry in sqlite_schema_rows]
    print(" ".join(names))
elif "count(*)" in command:
    table_name = sys.argv[2].split(" ")[-1]
    sqlite_schema_rows = read_sqlite_schema_rows()

    rootpage = None
    num_columns = None
    for schema_row in sqlite_schema_rows:
        name = schema_row['name'].decode()
        if name == table_name:
            rootpage = schema_row['rootpage']
            sql = schema_row['sql'].decode()
            values = sql[sql.index('('):sql.index(')')]
            num_columns = len(values.split(','))

    if not rootpage:
        print('no rootpage found')

    if not num_columns:
        print('no num_columns found')

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), "big")

        database_file.seek(page_size * (rootpage - 1))
        page_header = PageHeader.parse_from(database_file)

        print(page_header.number_of_cells)
elif "select" in command or "SELECT" in command:
    select_statement = sys.argv[2].split(" ")
    where_keyword_lower = 'where'
    where_keyword_upper = where_keyword_lower.upper()
    if where_keyword_lower in select_statement:
        where_keyword = where_keyword_lower
    elif where_keyword_upper in select_statement:
        where_keyword = where_keyword_upper
    filters = None
    if where_keyword_lower in select_statement or where_keyword_upper in select_statement:
        where_clause_idx = sys.argv[2].index(where_keyword)
        filter_clause = sys.argv[2][where_clause_idx + len(where_keyword):].strip()
        filters = [filter.strip().replace("'", "") for filter in filter_clause.split('=')]

        select_statement = sys.argv[2][:where_clause_idx].strip().split(" ")

    table_name = select_statement[-1]
    column_names = [term.replace(',', '') for term in select_statement[1:-1]]

    sqlite_schema_rows = read_sqlite_schema_rows()

    index_rootpage = None
    for schema_row in sqlite_schema_rows:
        if schema_row['type'].decode() == 'index':  # or schema_row['name'].decode() == "idx_companies_country"
            index_rootpage = schema_row['rootpage']

    rootpage = None
    column_pos = []
    num_columns = None
    all_columns = []
    for schema_row in sqlite_schema_rows:
        name = schema_row['name'].decode()
        if name == table_name:
            rootpage = schema_row['rootpage']
            sql = schema_row['sql'].decode()
            values = sql[sql.index('('):sql.index(')')]
            columns = values.split(',')
            num_columns = len(columns)
            for column_name in column_names:
                for i in range(0, num_columns):
                    if column_name in columns[i]:
                        column_pos.append(i)
    col_str = sql[sql.index('('):sql.index(')')].split(',')
    all_columns = [s.strip().replace('(', '').split(' ')[0] for s in col_str]
    if not rootpage:
        print('no rootpage found')

    if not column_pos:
        print('no column_pos found')

    if not num_columns:
        print('no num_columns found')

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), "big")

    def read_table_leaf_or_interior(page_number: int):
        with open(database_file_path, "rb") as database_file:
            b_tree_leaf_page_offset = page_size * (page_number - 1)
            database_file.seek(b_tree_leaf_page_offset)
            page_header = PageHeader.parse_from(database_file)

            if page_header.page_type == 5:
                database_file.seek(b_tree_leaf_page_offset + 12)
                cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

                for cell_pointer in cell_pointers:
                    database_file.seek(b_tree_leaf_page_offset + cell_pointer)

                    left_child_pointer = int.from_bytes(database_file.read(4), "big")
                    key = parse_varint(database_file)
                    read_table_leaf_or_interior(left_child_pointer)

                read_table_leaf_or_interior(page_header.right_most_pointer)

            elif page_header.page_type == 13:  # A value of 13 (0x0d) means the page is a leaf table b-tree page.
                # skip header
                database_file.seek(b_tree_leaf_page_offset + 8)

                cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

                column_values = []
                for cell_pointer in cell_pointers:
                    database_file.seek(b_tree_leaf_page_offset + cell_pointer)
                    _number_of_bytes_in_payload = parse_varint(database_file)
                    rowid = parse_varint(database_file)
                    record = parse_record(database_file, num_columns)
                    if 'id' in column_names:
                        record[0] = rowid

                    out = [record[col_pos] for col_pos in column_pos]

                    if filters:
                        # assume only single where clause
                        filter_idx = all_columns.index(filters[0])
                        if filter_idx != -1:
                            if record[filter_idx] and record[filter_idx].decode() == filters[1]:
                                column_values.append(out)
                    else:
                        column_values.append(out)

                def format_value(value):
                    if value is None:
                        return ""

                    if isinstance(value, int):
                        return str(value)

                    return value.decode()

                for col_val in column_values:
                    print('|'.join(format_value(col) for col in col_val))
            else:
                print('unknown page_type', page_header.page_type)

    def read_index_leaf_or_interior(page_number: int):
        with open(database_file_path, "rb") as database_file:
            b_tree_leaf_page_offset = page_size * (page_number - 1)
            database_file.seek(b_tree_leaf_page_offset)
            page_header = PageHeader.parse_from(database_file)

            if page_header.page_type == 2:
                database_file.seek(b_tree_leaf_page_offset + 12)
                cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in
                                 range(page_header.number_of_cells)]

                row_ids = []
                for cell_pointer in cell_pointers:
                    database_file.seek(b_tree_leaf_page_offset + cell_pointer)
                    left_child_pointer = int.from_bytes(database_file.read(4), "big")
                    _number_of_bytes_in_payload = parse_varint(database_file)
                    record = parse_record(database_file, 2)

                    key = (record[0] or b"").decode()
                    value_to_filter_by = filters[1]

                    if key == value_to_filter_by:
                        row_ids.append(record[1])

                    if key >= value_to_filter_by:
                        row_ids += read_index_leaf_or_interior(left_child_pointer)

                        if key > value_to_filter_by:
                            break

                if key <= value_to_filter_by:
                    row_ids += read_index_leaf_or_interior(page_header.right_most_pointer)

                return row_ids
            elif page_header.page_type == 10:
                database_file.seek(b_tree_leaf_page_offset + 8)
                cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in
                                 range(page_header.number_of_cells)]

                row_ids = []
                for cell_pointer in cell_pointers:
                    database_file.seek(b_tree_leaf_page_offset + cell_pointer)
                    _number_of_bytes_in_payload = parse_varint(database_file)
                    record = parse_record(database_file, 2)

                    key = (record[0] or b"").decode()
                    value_to_filter_by = filters[1]

                    if key == value_to_filter_by:
                        row_ids.append(record[1])
                return row_ids
            else:
                print('unknown page_type', page_header.page_type)

    def read_one_table_row(page_number, row_id):
        with open(database_file_path, "rb") as database_file:
            b_tree_leaf_page_offset = page_size * (page_number - 1)
            database_file.seek(b_tree_leaf_page_offset)
            page_header = PageHeader.parse_from(database_file)

            if page_header.page_type == 5:
                database_file.seek(b_tree_leaf_page_offset + 12)
                cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

                for cell_pointer in cell_pointers:
                    database_file.seek(b_tree_leaf_page_offset + cell_pointer)

                    left_child_pointer = int.from_bytes(database_file.read(4), "big")
                    key = parse_varint(database_file)
                    if key >= row_id:
                        return read_one_table_row(left_child_pointer, row_id)

                return read_one_table_row(page_header.right_most_pointer, row_id)

            elif page_header.page_type == 13:  # A value of 13 (0x0d) means the page is a leaf table b-tree page.
                # skip header
                database_file.seek(b_tree_leaf_page_offset + 8)

                cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

                for cell_pointer in cell_pointers:
                    database_file.seek(b_tree_leaf_page_offset + cell_pointer)
                    _number_of_bytes_in_payload = parse_varint(database_file)
                    rowid = parse_varint(database_file)
                    if rowid == row_id:
                        record = parse_record(database_file, num_columns)
                        if 'id' in column_names:
                            record[0] = rowid
                        return record
                return None
            else:
                print('unknown page_type', page_header.page_type)

    if index_rootpage:
        row_ids = read_index_leaf_or_interior(index_rootpage)
        records = []
        for row_id in row_ids:
            record = read_one_table_row(rootpage, row_id)
            filtered_records = [record[col_pos] for col_pos in column_pos]
            records.append(filtered_records)

        def format_value(value):
            if value is None:
                return ""

            if isinstance(value, int):
                return str(value)

            return value.decode()

        for col_val in records:
            print('|'.join(format_value(col) for col in col_val))
    else:
        read_table_leaf_or_interior(rootpage)
else:
    print(f"Invalid command: {command}")

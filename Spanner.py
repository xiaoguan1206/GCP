import argparse
import base64
import datetime
import decimal
import logging

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types


def create_database(instance_id, database_id):
    """Creates a database and tables for sample data."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(
        database_id,
        ddl_statements=[
            """CREATE TABLE Singers (
            SingerId     INT64 NOT NULL,
            FirstName    STRING(1024),
            LastName     STRING(1024),
            SingerInfo   BYTES(MAX)
        ) PRIMARY KEY (SingerId)""",
            """CREATE TABLE Albums (
            SingerId     INT64 NOT NULL,
            AlbumId      INT64 NOT NULL,
            AlbumTitle   STRING(MAX)
        ) PRIMARY KEY (SingerId, AlbumId),
        INTERLEAVE IN PARENT Singers ON DELETE CASCADE""",
        ],
    )

    operation = database.create()

    print("Waiting for operation to complete...")
    operation.result(120)

    print("Created database {} on instance {}".format(database_id, instance_id))
''' - -- - - - - - - - - - - - - - - - - - - - - - - - - - ----- - - -------------------'''
    
# instance_id = "your-spanner-instance"
# database_id = "your-spanner-db-id"
spanner_client = spanner.Client()
instance = spanner_client.instance("test-instance")
database = instance.database("test-database")

def insert_singers(transaction):
    row_ct = transaction.execute_update(
        "INSERT employees (employee_id, name, start_date) VALUES "
        "(1, 'Steve Jobs', '1976-04-01'), "
        "(2, 'Bill Gates', '1975-04-04'), "
        "(3, 'Larry Page', '1998-09-04')"
    )
    print("{} record(s) inserted.".format(row_ct))

database.run_in_transaction(insert_singers) 




''' - -- - - - - - - - - - - - - - - - - - - - - - - - - - ----- - - -------------------'''
if __name__ == "__main__":  # noqa: C901
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("instance_id", help="Your Cloud Spanner instance ID.")
    parser.add_argument(
        "--database-id", help="Your Cloud Spanner database ID.", default="example_db"
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("create_instance", help=create_instance.__doc__)
    subparsers.add_parser("create_database", help=create_database.__doc__)
    subparsers.add_parser("insert_data", help=insert_data.__doc__)
    subparsers.add_parser("delete_data", help=delete_data.__doc__)
    subparsers.add_parser("query_data", help=query_data.__doc__)
    subparsers.add_parser("read_data", help=read_data.__doc__)
    subparsers.add_parser("read_stale_data", help=read_stale_data.__doc__)
    subparsers.add_parser("add_column", help=add_column.__doc__)
    subparsers.add_parser("update_data", help=update_data.__doc__)
    subparsers.add_parser(
        "query_data_with_new_column", help=query_data_with_new_column.__doc__
    )
    subparsers.add_parser("read_write_transaction", help=read_write_transaction.__doc__)
    subparsers.add_parser("read_only_transaction", help=read_only_transaction.__doc__)
    subparsers.add_parser("add_index", help=add_index.__doc__)
    query_data_with_index_parser = subparsers.add_parser(
        "query_data_with_index", help=query_data_with_index.__doc__
    )
    query_data_with_index_parser.add_argument("--start_title", default="Aardvark")
    query_data_with_index_parser.add_argument("--end_title", default="Goo")
    subparsers.add_parser("read_data_with_index", help=insert_data.__doc__)
    subparsers.add_parser("add_storing_index", help=add_storing_index.__doc__)
    subparsers.add_parser("read_data_with_storing_index", help=insert_data.__doc__)
    subparsers.add_parser(
        "create_table_with_timestamp", help=create_table_with_timestamp.__doc__
    )
    subparsers.add_parser(
        "insert_data_with_timestamp", help=insert_data_with_timestamp.__doc__
    )
    subparsers.add_parser("add_timestamp_column", help=add_timestamp_column.__doc__)
    subparsers.add_parser(
        "update_data_with_timestamp", help=update_data_with_timestamp.__doc__
    )
    subparsers.add_parser(
        "query_data_with_timestamp", help=query_data_with_timestamp.__doc__
    )
    subparsers.add_parser("write_struct_data", help=write_struct_data.__doc__)
    subparsers.add_parser("query_with_struct", help=query_with_struct.__doc__)
    subparsers.add_parser(
        "query_with_array_of_struct", help=query_with_array_of_struct.__doc__
    )
    subparsers.add_parser("query_struct_field", help=query_struct_field.__doc__)
    subparsers.add_parser(
        "query_nested_struct_field", help=query_nested_struct_field.__doc__
    )
    subparsers.add_parser("insert_data_with_dml", help=insert_data_with_dml.__doc__)
    subparsers.add_parser("log_commit_stats", help=log_commit_stats.__doc__)
    subparsers.add_parser("update_data_with_dml", help=update_data_with_dml.__doc__)
    subparsers.add_parser("delete_data_with_dml", help=delete_data_with_dml.__doc__)
    subparsers.add_parser(
        "update_data_with_dml_timestamp", help=update_data_with_dml_timestamp.__doc__
    )
    subparsers.add_parser(
        "dml_write_read_transaction", help=dml_write_read_transaction.__doc__
    )
    subparsers.add_parser(
        "update_data_with_dml_struct", help=update_data_with_dml_struct.__doc__
    )
    subparsers.add_parser("insert_with_dml", help=insert_with_dml.__doc__)
    subparsers.add_parser(
        "query_data_with_parameter", help=query_data_with_parameter.__doc__
    )
    subparsers.add_parser(
        "write_with_dml_transaction", help=write_with_dml_transaction.__doc__
    )
    subparsers.add_parser(
        "update_data_with_partitioned_dml",
        help=update_data_with_partitioned_dml.__doc__,
    )
    subparsers.add_parser(
        "delete_data_with_partitioned_dml",
        help=delete_data_with_partitioned_dml.__doc__,
    )
    subparsers.add_parser("update_with_batch_dml", help=update_with_batch_dml.__doc__)
    subparsers.add_parser(
        "create_table_with_datatypes", help=create_table_with_datatypes.__doc__
    )
    subparsers.add_parser("insert_datatypes_data", help=insert_datatypes_data.__doc__)
    subparsers.add_parser("query_data_with_array", help=query_data_with_array.__doc__)
    subparsers.add_parser("query_data_with_bool", help=query_data_with_bool.__doc__)
    subparsers.add_parser("query_data_with_bytes", help=query_data_with_bytes.__doc__)
    subparsers.add_parser("query_data_with_date", help=query_data_with_date.__doc__)
    subparsers.add_parser("query_data_with_float", help=query_data_with_float.__doc__)
    subparsers.add_parser("query_data_with_int", help=query_data_with_int.__doc__)
    subparsers.add_parser("query_data_with_string", help=query_data_with_string.__doc__)
    subparsers.add_parser(
        "query_data_with_timestamp_parameter",
        help=query_data_with_timestamp_parameter.__doc__,
    )
    subparsers.add_parser(
        "query_data_with_query_options", help=query_data_with_query_options.__doc__
    )
    subparsers.add_parser(
        "create_client_with_query_options",
        help=create_client_with_query_options.__doc__,
    )

    args = parser.parse_args()

    if args.command == "create_instance":
        create_instance(args.instance_id)
    elif args.command == "create_database":
        create_database(args.instance_id, args.database_id)
    elif args.command == "insert_data":
        insert_data(args.instance_id, args.database_id)
    elif args.command == "delete_data":
        delete_data(args.instance_id, args.database_id)
    elif args.command == "query_data":
        query_data(args.instance_id, args.database_id)
    elif args.command == "read_data":
        read_data(args.instance_id, args.database_id)
    elif args.command == "read_stale_data":
        read_stale_data(args.instance_id, args.database_id)
    elif args.command == "add_column":
        add_column(args.instance_id, args.database_id)
    elif args.command == "update_data":
        update_data(args.instance_id, args.database_id)
    elif args.command == "query_data_with_new_column":
        query_data_with_new_column(args.instance_id, args.database_id)
    elif args.command == "read_write_transaction":
        read_write_transaction(args.instance_id, args.database_id)
    elif args.command == "read_only_transaction":
        read_only_transaction(args.instance_id, args.database_id)
    elif args.command == "add_index":
        add_index(args.instance_id, args.database_id)
    elif args.command == "query_data_with_index":
        query_data_with_index(
            args.instance_id, args.database_id, args.start_title, args.end_title
        )
    elif args.command == "read_data_with_index":
        read_data_with_index(args.instance_id, args.database_id)
    elif args.command == "add_storing_index":
        add_storing_index(args.instance_id, args.database_id)
    elif args.command == "read_data_with_storing_index":
        read_data_with_storing_index(args.instance_id, args.database_id)
    elif args.command == "create_table_with_timestamp":
        create_table_with_timestamp(args.instance_id, args.database_id)
    elif args.command == "insert_data_with_timestamp":
        insert_data_with_timestamp(args.instance_id, args.database_id)
    elif args.command == "add_timestamp_column":
        add_timestamp_column(args.instance_id, args.database_id)
    elif args.command == "update_data_with_timestamp":
        update_data_with_timestamp(args.instance_id, args.database_id)
    elif args.command == "query_data_with_timestamp":
        query_data_with_timestamp(args.instance_id, args.database_id)
    elif args.command == "write_struct_data":
        write_struct_data(args.instance_id, args.database_id)
    elif args.command == "query_with_struct":
        query_with_struct(args.instance_id, args.database_id)
    elif args.command == "query_with_array_of_struct":
        query_with_array_of_struct(args.instance_id, args.database_id)
    elif args.command == "query_struct_field":
        query_struct_field(args.instance_id, args.database_id)
    elif args.command == "query_nested_struct_field":
        query_nested_struct_field(args.instance_id, args.database_id)
    elif args.command == "insert_data_with_dml":
        insert_data_with_dml(args.instance_id, args.database_id)
    elif args.command == "log_commit_stats":
        log_commit_stats(args.instance_id, args.database_id)
    elif args.command == "update_data_with_dml":
        update_data_with_dml(args.instance_id, args.database_id)
    elif args.command == "delete_data_with_dml":
        delete_data_with_dml(args.instance_id, args.database_id)
    elif args.command == "update_data_with_dml_timestamp":
        update_data_with_dml_timestamp(args.instance_id, args.database_id)
    elif args.command == "dml_write_read_transaction":
        dml_write_read_transaction(args.instance_id, args.database_id)
    elif args.command == "update_data_with_dml_struct":
        update_data_with_dml_struct(args.instance_id, args.database_id)
    elif args.command == "insert_with_dml":
        insert_with_dml(args.instance_id, args.database_id)
    elif args.command == "query_data_with_parameter":
        query_data_with_parameter(args.instance_id, args.database_id)
    elif args.command == "write_with_dml_transaction":
        write_with_dml_transaction(args.instance_id, args.database_id)
    elif args.command == "update_data_with_partitioned_dml":
        update_data_with_partitioned_dml(args.instance_id, args.database_id)
    elif args.command == "delete_data_with_partitioned_dml":
        delete_data_with_partitioned_dml(args.instance_id, args.database_id)
    elif args.command == "update_with_batch_dml":
        update_with_batch_dml(args.instance_id, args.database_id)
    elif args.command == "create_table_with_datatypes":
        create_table_with_datatypes(args.instance_id, args.database_id)
    elif args.command == "insert_datatypes_data":
        insert_datatypes_data(args.instance_id, args.database_id)
    elif args.command == "query_data_with_array":
        query_data_with_array(args.instance_id, args.database_id)
    elif args.command == "query_data_with_bool":
        query_data_with_bool(args.instance_id, args.database_id)
    elif args.command == "query_data_with_bytes":
        query_data_with_bytes(args.instance_id, args.database_id)
    elif args.command == "query_data_with_date":
        query_data_with_date(args.instance_id, args.database_id)
    elif args.command == "query_data_with_float":
        query_data_with_float(args.instance_id, args.database_id)
    elif args.command == "query_data_with_int":
        query_data_with_int(args.instance_id, args.database_id)
    elif args.command == "query_data_with_string":
        query_data_with_string(args.instance_id, args.database_id)
    elif args.command == "query_data_with_timestamp_parameter":
        query_data_with_timestamp_parameter(args.instance_id, args.database_id)
    elif args.command == "query_data_with_query_options":
        query_data_with_query_options(args.instance_id, args.database_id)
    elif args.command == "create_client_with_query_options":
        create_client_with_query_options(args.instance_id, args.database_id)

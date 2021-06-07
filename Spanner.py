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
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

def insert_singers(transaction):
    row_ct = transaction.execute_update(
        "INSERT employees (employee_id, name, start_date) VALUES "
        "(1, 'Steve Jobs', '1976-04-01'), "
        "(2, 'Bill Gates', '1975-04-04'), "
        "(3, 'Larry Page', '1998-09-04')"
    )
    print("{} record(s) inserted.".format(row_ct))

database.run_in_transaction(insert_singers) 

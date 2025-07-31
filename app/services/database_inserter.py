from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any

class RealDatabaseInserter:
    """
    A service class responsible for connecting to an external database
    and inserting synthetic data in batches.
    """
    def insert_batch(self, connection_details: str, table_name: str, data: List[Dict[str, Any]]):
        """
        Connects to the target database and performs a bulk insert of the provided data.

        Args:
            connection_details (str): The SQLAlchemy connection string for the target database.
            table_name (str): The name of the table to insert data into.
            data (List[Dict[str, Any]]): A list of dictionaries, where each dictionary represents a row.
        """
        if not data:
            print("--- DATABASE INSERTER (REAL) ---")
            print("No data provided to insert. Skipping.")
            return True

        print(f"--- DATABASE INSERTER (REAL) ---")
        print(f"Connecting to target database...")
        
        target_engine = None
        db_session = None
        try:
            # 1. Create a new engine for the target database
            target_engine = create_engine(connection_details)
            
            # 2. Perform the insert operation using a single transaction
            with target_engine.connect() as connection:
                with connection.begin() as transaction:
                    try:
                        print(f"Inserting {len(data)} records into table: '{table_name}'")
                        
                        # Use SQLAlchemy Core for efficient bulk inserting.
                        # This is much faster than row-by-row insertion.
                        # We reflect the table structure from the database if it exists.
                        metadata = MetaData()
                        table = Table(table_name, metadata, autoload_with=target_engine)
                        
                        connection.execute(table.insert(), data)
                        
                        transaction.commit()
                        print(f"Successfully inserted records into '{table_name}'.")
                    except Exception as e:
                        print(f"ERROR: Transaction failed. Rolling back. Error: {e}")
                        transaction.rollback()
                        raise e

        except Exception as e:
            print(f"ERROR: Failed to connect or insert data into target database: {e}")
            raise e
        finally:
            # 4. Ensure the engine is properly disposed of to release connections
            if target_engine:
                target_engine.dispose()
                print("Target database engine disposed.")
        
        return True

# Create a singleton instance of the service to be imported elsewhere
db_inserter = RealDatabaseInserter()
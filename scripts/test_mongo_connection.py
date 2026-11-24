"""
Test MongoDB Connection
Connects to MongoDB and prints collection names and document counts.
Usage: python scripts/test_mongo_connection.py
"""

import os
import sys

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


def main():
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    database = os.getenv("MONGO_DATABASE", "bigdata")

    print(f"Connecting to MongoDB: {uri}")

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Force a connection check
        client.admin.command("ping")
        print("Connection successful!\n")
    except ConnectionFailure:
        print(f"ERROR: Could not connect to MongoDB at {uri}")
        sys.exit(1)

    db = client[database]

    print(f"Database: {database}")
    print(f"Collections:")
    print("-" * 45)

    collections = db.list_collection_names()
    if not collections:
        print("  (no collections found)")
    else:
        for name in sorted(collections):
            count = db[name].count_documents({})
            indexes = list(db[name].list_indexes())
            print(f"  {name}:")
            print(f"    Documents: {count}")
            print(f"    Indexes:   {len(indexes)}")
            for idx in indexes:
                print(f"      - {idx['name']}: {dict(idx['key'])}")
            print()

    client.close()
    print("Done.")


if __name__ == "__main__":
    main()

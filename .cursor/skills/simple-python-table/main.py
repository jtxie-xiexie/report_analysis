import pandas as pd


def get_user_table() -> pd.DataFrame:
    """Return a very simple table."""
    return pd.DataFrame(
        [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 88},
            {"id": 3, "name": "Cathy", "score": 92},
        ]
    )


if __name__ == "__main__":
    table = get_user_table()
    print(table)

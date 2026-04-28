import os
from dotenv import load_dotenv

# Force reload of environment variables
load_dotenv(override=True)

class Config:
    def __init__(self):
        # Database
        self.MSSQL_SERVER = os.getenv("MSSQL_SERVER", "localhost")
        self.MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "nk_proteins")
        self.MSSQL_USER = os.getenv("MSSQL_USER", "sa")
        self.MSSQL_PASS = os.getenv("MSSQL_PASS", "")
        self.MSSQL_PORT = os.getenv("MSSQL_PORT", "1433")
        
        # Table
        self.TABLE_SALES = os.getenv("TABLE_SALES", "fact_sales")
        
        # Ground Truth Columns (V3)
        self.COL_NET_AMOUNT = os.getenv("COL_NET_AMOUNT", "Gross Value")
        self.COL_MATERIAL = os.getenv("COL_MATERIAL", "Material Code")
        self.COL_PRODUCT_NAME = os.getenv("COL_PRODUCT_NAME", "Material Desc")
        self.COL_QUANTITY = os.getenv("COL_QUANTITY", "Bill Qty")
        self.COL_DATE = os.getenv("COL_DATE", "Billing Date")
        self.COL_CUSTOMER = os.getenv("COL_CUSTOMER", "Customer Name")
        self.COL_BILL_DOC = os.getenv("COL_BILL_DOC", "Bill Doc No")
        self.COL_DELIVERY_NO = os.getenv("COL_DELIVERY_NO", "Delivery No")
        self.COL_TRUCK_NO = os.getenv("COL_TRUCK_NO", "Truck No")
        self.COL_PRICE_UNIT = os.getenv("COL_PRICE_UNIT", "Price Per Unit")

    def get_column_map(self):
        """
        Returns a map of internal keys to database column names for validation.
        ONLY includes the 10 active Ground Truth columns.
        """
        return {
            "NET_AMOUNT": self.COL_NET_AMOUNT,
            "MATERIAL": self.COL_MATERIAL,
            "PRODUCT_NAME": self.COL_PRODUCT_NAME,
            "QUANTITY": self.COL_QUANTITY,
            "DATE": self.COL_DATE,
            "CUSTOMER": self.COL_CUSTOMER,
            "BILL_DOC": self.COL_BILL_DOC,
            "DELIVERY_NO": self.COL_DELIVERY_NO,
            "TRUCK_NO": self.COL_TRUCK_NO,
            "PRICE_UNIT": self.COL_PRICE_UNIT
        }

# Global config instance
config = Config()

# AI Schema Context for Ad-hoc Generation (V3 Floor 1 Fallback)
SQL_SCHEMA = f"""
TABLE: {config.TABLE_SALES}
COLUMNS:
- {config.COL_DATE} (datetime): The billing/transaction date.
- {config.COL_CUSTOMER} (string): Customer name.
- {config.COL_MATERIAL} (string): Material/Product code.
- {config.COL_PRODUCT_NAME} (string): Material description/Product name.
- {config.COL_QUANTITY} (decimal): Quantity billed.
- {config.COL_NET_AMOUNT} (decimal): Gross/Net revenue value.
- {config.COL_PRICE_UNIT} (decimal): Price per unit.
- {config.COL_BILL_DOC} (string): Billing document number.
"""

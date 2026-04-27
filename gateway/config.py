import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

class Config:
    # Database Table
    TABLE_SALES = os.getenv("TABLE_SALES", "fact_sales")

    # Column Mapping
    COL_NET_AMOUNT = os.getenv("COL_NET_AMOUNT", "NetAmount")
    COL_COST_AMOUNT = os.getenv("COL_COST_AMOUNT", "CostAmount")
    COL_REGION = os.getenv("COL_REGION", "CustomerRegionName")
    COL_PLANT = os.getenv("COL_PLANT", "PlantCityName")
    COL_MATERIAL = os.getenv("COL_MATERIAL", "Material")
    COL_PRODUCT_NAME = os.getenv("COL_PRODUCT_NAME", "ProductName")
    COL_UNIT = os.getenv("COL_UNIT", "BillingQuantityUnit")
    COL_QUANTITY = os.getenv("COL_QUANTITY", "BillingQuantity")
    COL_DATE = os.getenv("COL_DATE", "BillingDocumentDate")
    COL_SALES_OFFICE = os.getenv("COL_SALES_OFFICE", "SalesOfficeName")
    COL_CUSTOMER = os.getenv("COL_CUSTOMER", "CustomerName")
    COL_BILL_DOC = os.getenv("COL_BILL_DOC", "Bill Doc No")
    COL_DELIVERY_NO = os.getenv("COL_DELIVERY_NO", "Delivery No")
    COL_TRUCK_NO = os.getenv("COL_TRUCK_NO", "Truck No")
    COL_PRICE_UNIT = os.getenv("COL_PRICE_UNIT", "Price Per Unit")

    def get_column_map(self):
        """Returns a dictionary of all mapped columns for validation."""
        return {
            "NET_AMOUNT": self.COL_NET_AMOUNT,
            "COST_AMOUNT": self.COL_COST_AMOUNT,
            "REGION": self.COL_REGION,
            "PLANT": self.COL_PLANT,
            "MATERIAL": self.COL_MATERIAL,
            "PRODUCT_NAME": self.COL_PRODUCT_NAME,
            "UNIT": self.COL_UNIT,
            "QUANTITY": self.COL_QUANTITY,
            "DATE": self.COL_DATE,
            "SALES_OFFICE": self.COL_SALES_OFFICE,
            "CUSTOMER": self.COL_CUSTOMER,
            "BILL_DOC": self.COL_BILL_DOC,
            "DELIVERY_NO": self.COL_DELIVERY_NO,
            "TRUCK_NO": self.COL_TRUCK_NO,
            "PRICE_UNIT": self.COL_PRICE_UNIT
        }

# Singleton instance
config = Config()

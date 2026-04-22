# SAP Sales Data Schema Reference (POC V2 - Updated)

Total Columns: 33

| Column Name | Description |
| :--- | :--- |
| BillingDocument | SAP Billing Document Number |
| BillingDocumentItem | Line item index |
| **BillingDocumentDate** | **Actual Invoice Date** |
| Material | Material ID |
| MaterialGroup | Group ID |
| MaterialGroupName | Category Name |
| MaterialType | Type ID |
| MaterialTypeName | Type description |
| ProductName | Product description |
| SalesOrganization | Org ID |
| SalesOrganizationName | Org description |
| DistributionChannel | Channel ID |
| DistributionChannelName | Channel description |
| Division | Division ID |
| DivisionName | Division description |
| SalesOffice | Office ID |
| SalesOfficeName | Office description |
| Plant | Plant ID |
| PlantName | Plant description |
| PlantCityName | City |
| PlantStreetName | Street Address |
| PlantPostalCode | Postal Code |
| **SoldToParty** | **Customer ID** |
| CustomerName | Customer description |
| **CustomerRegionName** | **Region (e.g. Gujarat)** |
| ProfitCenter | Profit Center ID |
| ProfitCenterName | PC Description |
| BillingQuantity | Quantity |
| BillingQuantityUnit | Unit of Measure |
| TransactionCurrency | Currency |
| **NetAmount** | **Revenue** |
| **CostAmount** | **Cost** |
| **GrossMargin** | **Margin** |

### Mapping Updates:
- **Date** = `BillingDocumentDate`
- **Region** = `CustomerRegionName`
- **Customer** = `SoldToParty`
- **Product** = `ProductName`
- **Revenue** = `NetAmount`

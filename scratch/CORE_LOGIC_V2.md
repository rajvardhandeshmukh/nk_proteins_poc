# NK Proteins CoPilot - Core Logic V2 (The 12 Rules)

## 1. Data Grain
- **Transaction-Level**: Each row = one unique transaction.
- **Primary Key**: `BillingDocument  (assumed unique per row in current dataset)`.
- **Constraint**: No duplicates, no joins required.

## 2. Revenue Logic (The Golden Rule)
- **Formula**: `Revenue = NetAmount`.
- **Pricing Truth**: Each invoice carries its own pricing truth. `NetAmount` is the final answer and includes all pricing conditions.
- **Constraint**: **Do not try to reconstruct revenue.** Never derive it from quantity or assumed rates.
- **Normalization**: Always in INR.
- **Rule**: `NetAmount` is always present, positive, and fully additive across all dimensions.

## 3. Profitability Logic (Two Tiers)
- **GrossMargin_All**: `NetAmount - CostAmount` (All rows).
- **GrossMargin_Valid**: `NetAmount - CostAmount` (Only where `CostAmount > 0`).
- **Caveat**: `Profitability is PARTIAL and must not be interpreted as full business margin across the dataset.`.

## 4. Quantity Logic
- **Source**: `BillingQuantity`.
- **Rule**: Quantity must be interpreted along with `BillingQuantityUnit` and cannot be compared or summed across different units.

## 5. Unit Handling (CRITICAL)
- **Aggregation Requirement**: All aggregations involving quantity **MUST** include `BillingQuantityUnit` in the grouping key.
- **Rule**: Never aggregate different units together (e.g., No KG + EA).
- **UI Behavior**: The same product may appear in multiple rows if sold in different units.

## 6. Product Logic
- **Identity**: `Material` (SKU) -> `ProductName` (1:1).
- **Rule**: Stable identifier, no ambiguity, no duplication.

## 7. Demand Logic (Customer)
- **Region**: `CustomerRegionName`.
- **Source**: Derived from `SoldToParty`.
- **Definition**: Represents the location of demand.

## 8. Supply Logic (Plant)
- **Supply Location**: `PlantCityName`.
- **Constraint**: Demand (`CustomerRegionName`) and Supply (`PlantCityName`) Queries must operate in either Demand view (CustomerRegionName) or Supply view (PlantCityName), unless explicitly specified.

## 9. Time Logic
- **Source**: `BillingDocumentDate`.
- **Usage**: Monthly trends and Yearly aggregation.

## 10. Aggregation Rules
- **Mandatory Grouping**: `BillingQuantityUnit` is mandatory whenever quantity is aggregated.
- **Optional Grouping**: `CustomerRegionName` (Demand), `PlantCityName` (Supply), Time.
- **Prohibited**: Aggregating quantity without unit; mixing demand and supply unintentionally.
- **Recommended Grouping**: `Material`, `ProductName`, `BillingQuantityUnit (mandatory when quantity is used)`

## 11. Ranking
- **Default**: Top 10.

## 12. Intentionally Ignored
- `ReturnsQuantity`, `TaxAmount`, `DistributionChannel`, `ProfitCenter`, `MaterialType`.

## 13. Pricing Truth
- Revenue must always be taken directly from `NetAmount`. Any calculation such as `Quantity × Rate` is invalid.
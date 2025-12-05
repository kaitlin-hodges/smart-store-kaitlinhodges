# P7 Custom BI Project

## 1. The Business Goal
Determine the low-performing products for possible discontinuation within their regions and analyze whether discounts hurt or help sales performance.
## 2. Data Source
Datawarehouse
### Sales
**Columns Used**
- Transaction ID
- SaleDate
- CustomerID
- ProductId
- SaleAmount
- DiscountPct
- PaymentTYpe
### Product
**Columns Used**
- Product ID
- Product Name
- Category
- UnitPrice
### Customer
**Columns Used**
- CustomerID
- Name
- Retion
## 3. Tools Used:
- Excel
- VScode
- Data warehouse
- PowerBI
- Python
## 4. Workflow & Logic
- I reviewed whether discounting helped increase sales for the lowest performing products across all regions.
- Discount percentage and total revenue in each region.
- Disount percentages, total revenue and performance of the products by region.
- I found that most products, a discount did not necessarily make the customer(s) interested in purchasing the product.
- If a higher discount was given, there was interest shown so something prmotional could be considered about those products.
- Smaller percentages did not improve selling the items.
- This shows that if they don't discontinue items, they should probably look at a price change on these low performing items.
## 5. Results (narrative + visualizations)

<div align="center">

## Central

</div>

![Dice Region Product](src/analytics_project/olap/finalolap/results/low_perf_region_central.png)

<div align="center">

## East

</div>

![Dice Region Product](src/analytics_project/olap/finalolap/results/low_perf_region_east.png)

## North

</div>

![Dice Region Product](src/analytics_project/olap/finalolap/results/low_perf_region_north.png)

## South

</div>

![Dice Region Product](src/analytics_project/olap/finalolap/results/low_perf_region_south.png)

## West

</div>

![Dice Region Product](src/analytics_project/olap/finalolap/results/low_perf_region_west.png)

## Discount Impact

</div>

![Dice Region Product](src/analytics_project/olap/finalolap/results/discount_impact_low_performers.png)

## Power BI Visual
## <img src="src/analytics_project/images/P7.png" width="1000">

## 6. Suggested Business Action
- There are a few items that have a higher revenue that could possibly be continued and see success if they consider a price change.
- The higher the discount given, the more success there was but that also shows a price change may be necessary.
- Discounts that were given did not save all products, just helped the business sell inventory.  The product probably had slow sales no matter the price.
- Suggestions would be given to disconintue the items that are towards the lower part of the graph.

## 7. Challenges
Challenges I ran into this week were that I wanted to keep last weeks files and results seperate from this weeks so I created another folder. It was a very simple fix but I struggled with getting code to run andwhen I eventually figured out the paths, all went well. I also had an issue with figuring out a capitalization issue where one was capitalized, the other was not and with something so small, it was hard to find.

## 8. Ethical Considerations
It is very important for a business to ensure that they are making decisions that are based on accurate data and not biased in any way.  This could occur with incomplete
information.  Automation and BI insights can only be relied on so much for support, they cannot replace human judgement when it

Update README.md with this weeks process.
git add .
git commit -m "Completed Custom BI Project, Analysis and Visualizations. Updated README"
git push -u origin main

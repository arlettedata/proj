### 0. Introduction

There are 11 examples in this tutorial, and you can interactively run the given **Input:** blocks and see the same thing shown in the **Output:** blocks.

This tutorial is intended to be interactively executed from the `tutorial` subdirectory. 

While **proj** works with semi-structured text files, i.e. XML and JSON, we focus on mainly on the command syntax and and querying abilities with provided CSV files.  As long as XML element (or JSON property) names match the ones used in this tutorial, those file formats would also serve as valid scenarios.  More information is forthcoming on working with these semi-structured file formats.

<br>

- - -

### 1. Build and deploy proj (in parent directory), if not done already.
Input:
```
make deploy
cd tutorial
```

<br>

- - -

### 2. Refer to the given names in header row.
Input:
```
cat orders.csv | head -1
```
Output:
```
Row ID,OrderID,Order Date,Ship Date,Ship Mode,Customer ID,Customer Name,Segment,Country,City,State,Postal Code,Region,Product ID,Category,Sub-Category,Product Name,Sales,Quantity,Discount,Profit
```

<br>

- - -

### 3. Look at the first five orders and customer names, using an input file rather than stdin.
Input:
```
proj --in=orders.csv Order\ Date Customer\ Name first[5]
```
Output:
```
Order Date,Customer Name
1/4/13,Phillina Ober
1/4/13,Phillina Ober
1/4/13,Phillina Ober
1/5/13,Mick Brown
1/6/13,Lycoris Saunders
```
Explanation:<br> there is no difference whether we use stdin or the `--in` parameter, except in certain buffering cases where **proj** will complain.

**proj** does not interpret file name extensions to determine format.  Instead, the tool guesses the input format, in order, from JSON, XML, log files (using typical Log4J formatting), tab-separated (TSV), and comma-separated (CSV). By the time we interpret as CSV, any file qualifies with garbage-in-garbage-out behavior.  Embedded JSON within log files are expanded.

`first[5]` is a "directive," i.e. a specification that does not result in its own output CSV column, that tells **proj** to only look at the first 5 rows of the input.  This is similar to `top[n]` which instead applies the cutoff after filtering and sorting has occured. 

The escaping of the space allows a match against the input `Customer Name` field.  An alternative way to express this is to use curly brackets and a string: `{"Customer Name"}`.  Curly bracket representation of paths becomes necessary for dealing with ambiguous characters, such as infix operators that appear in names.  (See Step 11.)

<br>

- - -

### 4. Provide custom header, while relaxing the case.
Input:
```
cat orders.csv | proj Date:order\ date Customer:customer\ name first[5]
```
Output:
```
Date,Customer
1/3/13,Darren Powers
1/4/13,Phillina Ober
1/4/13,Phillina Ober
1/4/13,Phillina Ober
1/5/13,Mick Brown
```
Explanation:<br> because we did not specify `--case=true` (equivalently, `case[true]`), everything is case insensitive, including path specifications, column names, and function names. 

**proj** will create default column names based on their expressions.  These are overriden by preceding the column expression with `name:`.

<br>

- - -

### 5. Show and then count the distinct customers.
Input:
```
proj --in=orders.csv name:customer\ name --distinct
```
Output:
```
name
Darren Powers
Phillina Ober
Mick Brown
... and 790 more rows
```
Explanation:<br> all column specifcations are generally expressions, with an alternative form for traditional command line flag syntax.  It is only a stylistic consideration whether one chooses, say, `--distinct` versus `distinct[]` or `--first=5` versus` first[5]`.

There is currently no support for the "COUNT DISTINCT" functionality that SQL offers.  Instead, we can make another pass by piping the result to a second invocations of **proj**:

Input:
```
proj --in=orders.csv --distinct name:Customer\ Name | proj count[name]
```
Output:
```
count[name]
793
```

<br>

- - -

### 6. Sum profit by segment with a custom header.
Input:
```
cat orders.csv | proj Segment \"Profit\ in\ \$1000\'s\":\"$\"\&round[sum[profit]/1000,2]\&\"K\"
```
Output:
```
Segment,Profit in $1000's
Consumer,$134.12K
Home Office,$60.3K
Corporate,$91.98K
```
Explanation:<br> There are a number of built-in functions with documentation forthcoming (until then, see `XmlOperatorFactory` in `xml_lib/xmlop.h` for a list.)  This example uses the function `round[expr,num-dec-places]` and the infix string concatenation operator `&`.  There is also a function `concat[str1,str2]` that is equivalent to `str1&str2`.

As we can see with all the character escaping clutter, we're fighting Bash a lot, which does its own tokenization of command line input before the rest is passed to **proj**.  One technique around this is to move arguments to a file, which is provided. 

Simplified input with a descending `sort` thrown in:
```
cat orders.csv | proj Segment @profitArg sort[-sum[profit]]
```
Output:
```Segment,Profit in $1000's
Consumer,$134.12K
Home Office,$60.3K
Corporate,$91.98K
```
Explanation:<br> argument files, which are filenames prepended or appended with '@' are a way to reuse arguments, improve readability, and get around Bash escaping.

<br>

- - -

### 7. Query the top 10 customers that made the most orders, sorted first by descending order of number of orders, and then by customer's (first) name.
```
cat orders.csv | proj Customer:Customer\ Name Orders:count[OrderID] sort[-Orders,Customer] top[10]
```
Output: 
```
Customer,Orders
William Brown,37
Matt Abelman,34
John Lee,34
Paul Prost,34
Chloris Kastensmidt,32
Jonathan Doherty,32
Seth Vernon,32
Edward Hooks,32
Zuschuss Carroll,31
Arthur Prichep,31
```

Explanation:<br> `sort[]` takes multiple sort values, in the order of major sort values to minor sort values. Descending sort orders are accomplished through negation. The convention used to string values in desceding order is to first coerce using as a string and then use negation: e.g. sort

`count[]` is an aggregate function.  All non-aggregate columns are considered to be groups.

<br>

- - -

### 8. Show the total profit by state for the South region only.
Input:
```
cat orders.csv | proj State @profitArg where[region==\"South\"]
```
Output:
```
state,Profit in $1000's
Georgia,$16.25K
Kentucky,$11.2K
Virginia,$18.6K
Louisiana,$2.2K
South Carolina,$1.77K
Arkansas,$4.01K
Tennessee,$-5.34K
Florida,$-3.4K
North Carolina,$-7.49K
Mississippi,$3.17K
Alabama,$5.79K
```
Explanation:<br> string literals are given by quoted strings (escaped here due to Bash).  We are availing ourselves to case-insensitive comparisons to the region (the data uses "State") because we've not specified `--case=true`.

`where[pred-expr]` filters the output rows where the `pred-expr` value is true or non-zero.  It is possible to express multiple constraints, either using the logical AND infix operator `&&` or with multiple `where` directives.

<br>

- - -

### 9. Filter customer names with only one order, with discussion on compounding aggregations.
Input:
```
cat orders.csv | proj Name:customer\ name where[count[orderid]==1]
```
Output:
```
Name
Ricardo Emerson
Lela Donovan
Anthony O'Donnell
Carl Jackson
Jocasta Rupert
```
Discussion:<br>
Suppose we wanted a *count* of customers who only ordered one item. Those familar with Excel's SUMIF might try to say this:
``` 
cat orders.csv | proj Name:customer\ name sum[if[count[orderid]==1,1,0]]
```
But this is not supported, and we are told:
```
Aggregate functions cannot be composed
```
Instead, pipe the result and count that result instead:
```
cat orders.csv | proj Name:customer\ name where[count[orderid]==1] | proj count[Name]
```
Output:
```
count[Name]
5
```

<br>

- - -

### 10. Use the `join` operator to produce a report of ten product returns and their reasons.
Input:
```
cat orders.csv | proj join[returns.csv] where[orderid==right::orderid] Product\ Name Reason:right::Reason top[10]
```
Output:
```
Product Name,Reason
"Wirebound Service Call Books, 5 1/2"" x 4""",Product Description Inaccurate
"Eldon Expressions Desk Accessory, Wood Pencil Holder, Oak",Product Description Inaccurate
Staple-on labels,Product Description Inaccurate
GBC Plastic Binding Combs,Product Description Inaccurate
"Acco Pressboard Covers with Storage Hooks, 9 1/2"" x 11"", Executive Red",Incorrect Products Delivered
"GBC Twin Loop Wire Binding Elements, 9/16"" Spine, Black",Incorrect Products Delivered
Xerox 1957,Incorrect Products Delivered
EcoTones Memo Sheets,Incorrect Products Delivered
Belkin 6 Outlet Metallic Surge Strip,Incorrect Products Delivered
"Bush Heritage Pine Collection 5-Shelf Bookcase, Albany Pine Finish, *Special Order",Customer Dissatified With Product
```

Explanation:<br> there are three principal parts to this join:
1. A `join[path]` directive providing the path to a file (XML, JSON, CSV/TSV) with the columns to be joined.
2. Zero or more `where[pred]` constraints that relates values from the joined input with values from the main input. 
3. The use of scoping information to differentiate between the "left" and "right" scopes: `right::path`

Note how the output prints quotation marks from the original data using a pair of quotes.

**proj** will attempt to optimize joins by creating index tables to use in the evaluations of the `where` directives.

Only left joins are supported.  A right join is made by swapping the main and join file names.  By default, an inner join is performed. Outer joins are supported by providing an extra boolean value: `join[path,true]`.  

<br>

- - -

### 11. Use the `join` operator and aggregate the product return reasons by product category and sub-category, sorted by category.

```
cat orders.csv | proj Returns:join[returns.csv] where[orderid==Returns::orderid] Category {Sub-Category} count[orderid] sort[Category] outheader[false]
```
Explanation:<br> The aggregations are grouped by both Category and Sub-Category (which is surrounded by braces to avoid the otherwise subtraction interpretion).  

This example overrides the default scope name `right::` with `Returns::` by providing a column name on the `join` directive.

The choice of `orderId` field is arbitrary; when it comes to the `count` aggregations, any field usually works.  For example, `count[Returns::Reason]` suffices.

`outheader[false]` (equivalent to `--outheader=false`) tells **proj** to not output a CSV header.

<br>

- - -

### How to execute the processes by correctly providing the arguments

#### Running the Madax construction process
###### Examples:

to get all the countries and brands: no need to specify any country and brand
```
spark-submit pipeline/Madax.py --dateFrom "dd/MM/yyyy" --dateTo "dd/MM/yyyy" --writeHDFS "True"
```
or

to get specific countries and brands
```
spark-submit pipeline/Madax.py --dateFrom "dd/MM/yyyy" --dateTo "dd/MM/yyyy" --country "FR" --brand "AP" --writeHDFS "True"
```

#### Running the Samara construction process
###### Examples:

to get all the countries and brands: no need to specify any country and brand
```
spark-submit pipeline/Samara.py --dateFrom "dd/MM/yyyy" --dateTo "dd/MM/yyyy" --writeHDFS "True"
```
or

to get specific countries and all brands
```
spark-submit pipeline/Samara.py --dateFrom "dd/MM/yyyy" --dateTo "dd/MM/yyyy" --country "FR" --writeHDFS "True"
```

or 

to get specific countries and brands
```
spark-submit pipeline/Samara.py --dateFrom "dd/MM/yyyy" --dateTo "dd/MM/yyyy" --country "FR" --brand "CP" --writeHDFS "True"
```
new
```
spark-submit pipeline/Main.py --table "samara" -- create "vinpromo" --dateFrom "dd/MM/yyyy" --dateTo "dd/MM/yyyy" --country "FR" --brand "CP" --writeHDFS "True"
```

```
spark-submit pipeline/Main.py --table "madax" --dateFrom "dd/MM/yyyy" --dateTo "dd/MM/yyyy" --country "France" --brand "AP" --writeHDFS "True"
```
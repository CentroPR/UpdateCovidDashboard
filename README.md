# UpdateCovidDashboard
The following code update Centro's [COVID-19 Stateside dashboard](https://cunycis.maps.arcgis.com/apps/opsdashboard/index.html#/a410ef9be09745a69fe1e62f071ca721?mode=view)  dashboard daily.

# Scope of Code

The code is split into three python files.

1. covid_update.py: This python file contains classes that extract and transform data from [usafact.org](https://usafacts.org/) into a usable ArcGIS dashboard formats.
2. agol.py: This python file contains one class that allows users to pull items from ArcGIS online and overwrite the content.
3. main.py: This python file runs the covid update using the two python files above.

# Example:

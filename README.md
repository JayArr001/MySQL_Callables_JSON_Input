# MySQL_Callables_JSON_Input
Application that simulates a storefront which needs to communicate with a MySQL database. The database has 2 tables: Order (parent) and Order details (child).
</br>
This particular variation features the ability to input data from a .csv file and output it to JSON string. It is in conjunction with use of CallableStatments and DDL statements to create a new schema + tables.
</br>
As an additional challenge, some of the input orders had their dates purposefully formatted incorrectly. The code needed extra design to parse and decide if dates were properly formatted. If they weren't, the data following it was ignored.</br>
</br>
Alongside an IDE, MySQL workbench was used to set up the initial table and verify operation results. Sample orders that were used for testing were stored and accessed in a .csv file</br>
The .csv file used for unit testing will also be provided in this repository.

TRUNCATE stock, orderLine, item, district, warehouse,orders, customer;
\COPY item FROM './data/project_files/data_files/item.csv' WITH (FORMAT csv);
\COPY warehouse FROM './data/project_files/data_files/warehouse.csv' WITH (FORMAT csv);
\COPY stock FROM './data/project_files/data_files/stock.csv' WITH (FORMAT csv);

\COPY district FROM './data/project_files/data_files/district.csv' WITH (FORMAT csv);
\COPY customer FROM './data/project_files/data_files/customer.csv' WITH (FORMAT csv);
\COPY orders FROM './data/project_files/data_files/ordersaa' WITH (FORMAT csv);
\COPY orders FROM './data/project_files/data_files/ordersab' WITH (FORMAT csv);
\COPY orderLine FROM './data/project_files/data_files/order-line.csv' WITH (FORMAT csv, NULL "null");
# dim_bench_sql_oracle

A Framework for Dimensional Benchmarking of SQL Query Performance
    http://aprogrammerwrites.eu/?p=1833

Pre-requisites
==============
An Oracle database where you have sys access.

There are no other dependencies outside this project.

Output logging
==============
The testing utility packages use my own simple logging framework, installed as part of the installation scripts. To replace this with your own preferred logging framework, simply edit the procedure Utils.Write_Log to output using your own logging procedure, and optionally drop the log_headers and log_lines tables, along with the three Utils.*_Log methods.

As far as I know the code should work on any recent-ish version - I have tested on 11.2 and 12.1.

Install steps
=============
- This is best run initially in a private database where you have sys user access
- Clone the project and go to the relevant bat (pre-v12 versions) or bat_12c folder
- Update bench.bat and SYS.bat with any credentials differences on your system
- Check the Install_SYS.sql (Install_SYS_v11.sql) script, and ensure you have a write-able output folder with the same name as in the script
- Run Install_SYS.bat (Install_SYS_v11.bat) to create the bench user and output directory, and grant privileges
- Run Install_lib.bat to install general library utilities in the bench schema
- Run Install_bench.bat to install the benchmarking framework in the bench schema, along with the demo problem
- Run Install_bench_examples.bat (Install_bench_examples_v11.bat) to install the benchmarking framework in the bench schema, along with the demo problems
- Check log files for any errors
- Run Test_Bur.bat, or Batch_Bra.bat (etc.) for the demo problems


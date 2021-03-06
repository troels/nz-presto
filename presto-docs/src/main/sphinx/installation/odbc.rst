============
ODBC Drivers
============

Presto can be accessed using one of the Teradata ODBC
drivers. The ODBC drivers are available for Windows, Mac, and Linux. The
drivers are available for free download from
https://www.teradata.com/presto.

============================ ========================== =======================================
Teradata Presto ODBC Version Compatible Presto Versions Documentation
============================ ========================== =======================================
1.1.15                       0.179-t.x                  `Teradata Presto ODBC 1.1.15 Documention <http://teradata-presto.s3.amazonaws.com/odbc-1.1.15.1024/TeradataODBCDriverPrestoInstallGuide_1_1_15.pdf>`_

1.1.12                       0.157.1-t.x, 0.167-t.x     `Teradata Presto ODBC 1.1.12 Documention <http://teradata-presto.s3.amazonaws.com/odbc-1.1.12.1021/TeradataODBCDriverPrestoInstallGuide_1_1_12.pdf>`_

1.1.8                        0.157.1-t.x, 0.167-t.x     `Teradata Presto ODBC 1.1.8 Documention <http://teradata-presto.s3.amazonaws.com/odbc-1.1.8.1016/TeradataODBCDriverPrestoInstallGuide_1_1_8.pdf>`_

1.1.4                        0.152.1-t.x, 0.148-t.x     `Teradata Presto ODBC 1.1.4 Documention <http://teradata-presto.s3.amazonaws.com/odbc-1.1.4.1011/TeradataODBCDriverPrestoInstallGuide_1_1_4.pdf>`_

1.1.3                        0.148-t.x                  `Teradata Presto ODBC 1.1.3 Documention <http://teradata-presto.s3.amazonaws.com/odbc-1.1.3.1007/TeradataODBCDriverPrestoInstallGuide_1_1_3.pdf>`_

1.1.0                        0.141-t                    `Teradata Presto ODBC 1.1.0 Documention <http://teradata-presto.s3.amazonaws.com/odbc-1.1.0.1004/TeradataODBCDriverPrestoInstallGuide.pdf>`_

1.0.0                        0.127-t, 0.115-t, 0.101-t  `Teradata Presto ODBC 1.0.0 Documention <http://teradata-presto.s3.amazonaws.com/odbc-1.0.0.1001/TeradataODBCDriverPrestoInstallGuide.pdf>`_
============================ ========================== =======================================

System Requirements
-------------------

============================= ==============================================
Operating System              Driver Manager
============================= ==============================================
OS X 10.9, OS X 10.10         iODBC 3.52.7 or later

Windows 32 and 64             Microsoft ODBC Driver Manager

RHEL 6.x, CentOS equivalent   iODBC 3.52.7 or later, unixODBC 2.3.0 or later
============================= ==============================================

Tableau Customizations
----------------------

**Windows Only**
The Teradata client distribution package comes with a Tableau Datasource Connection (TDC) file. The file is used to better the use of Presto and Tableau by customizing the SQL that Tableau sends to Presto via the driver. The TDC will not correct functionality, it will only inform Tableau of the best way to work with the Teradata Presto ODBC driver. The TDC file is included in the Presto Client Package download.

After installing the ODBC driver on Windows, you should copy the TDC file to the location Tableau will look for it:
``C:\Users\<USER_NAME>\Documents\My Tableau Repository\Datasources``

**Tableau 10**
Tableau 10 provides a named connector for Presto. However, this connector only works for Presto 141t, and it should not be used.

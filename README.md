esget
=====
Earth System Grid gETer
-----------------------

esget is a tool to mirror part of the Earth System Grid Federation (ESGF) distributed database. The output of many climate-related simulation projects is distributed via ESGF, e.g. CORDEX, CMIP5, GeoMIP, or PMIP3.

esget is designed to deal with very large datasets. Its features include:

* tracking of local files
* accepts arbitrary dataset-definitions
* uses multiprocessing for ESGF-queries and downloading
* uses an arbitrary large number of hosts for parallel downloading
* state is kept in a SQLITE-database
* mostly Python

Status
------

This is beta-ware.

We successfully run esget to mirror a large part of CORDEX. The repo includes a sample configuration for a small subset of CORDEX, for testing.

The configuration file (config/corex_test.cfg) is very well documented and should get you started if you feel adventurous. However, other documentation is missing at the moment and we aim at a proper first release by the beginning of November 2014.
[Send me a note](https://github.com/hvwaldow) if you are interested to get a notification once we have a good release.

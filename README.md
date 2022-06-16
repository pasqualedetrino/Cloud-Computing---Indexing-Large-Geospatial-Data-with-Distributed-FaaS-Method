# Cloud Computing Project: - Indexing Large Geospatial Data with Distributed FaaS Method

This is our implementation of a faas-based system for the Cloud Computing exam. Our work is based on the following papers:

* [Klimatic: A Virtual Data Lake for Harvesting and Distribution of Geospatial Data](https://ieeexplore.ieee.org/abstract/document/7836565);
* [Serverless Workflows for Indexing Large Scientific Data](https://dl.acm.org/doi/pdf/10.1145/3366623.3368140?casa_token=KCe_SuOfBE4AAAAA:cG39nZJURBJaWFEQD7dal2eRnhlRmIeXm2R_u7DUYdIYomvFJOlMUokpNx1lEMDjnGL9T9ZzPT-e);

The system we designed uses faas, in particular the **funcX framework** ([Documentation](https://funcx.readthedocs.io/en/latest/)). The extraction of the metadata is carried out on files in **NetCDF** format ([Documentation](https://ieeexplore.ieee.org/stamp/stamp.jsp?arnumber=56302&casa_token=CO5-yol4ZKIAAAAA:p7EMtqK_SQwug7brk8NsEw4aQzcXUgX0guDFiMnQ8LIhbFICqPXhHVmywQrm1nj443Q0F7l0gw)). A DB (**PosgreSQL**) is used on a **MS Azure** server to store the links of the files that are later used to extract the data requested by the user.

## Architecture
Below is an image of the infrastructure of the system created:

![Infrastruttura](https://user-images.githubusercontent.com/47244184/173227320-9613d04b-914f-4486-8f30-acaa8d6f5beb.png)

As the image shows, the system is divided into two modules. The module on the left is responsible for calling up parsers via funcX and then executing the code remotely. Endpoints save the tuples they extract to the DB. The other module queries the DB using funcX.

## Installation and Execution
* Clone this repository and enter it:

```bash
git clone https://github.com/pasqualedetrino/Cloud-Computing---Indexing-Large-Geospatial-Data-with-Distributed-FaaS-Method.git
```
* Do not forget to enter the endpoint codes and change the parameters for connection to the DB;
* Install all requirements;
* Execute `ExtractMetadata.py` using `python` (version 3.7+). This is an example:

```bash
python ExtractMetadata.py
```

The `ExtractMetadata.py` file does not need input parameters as it performs direct parsers for each type of measurement.

* Execute `Query.py` using `python` (version 3.7+). This is an example:
```bash
python Query.py --lat_min <float> --lat_max <float> --long_min <float> --long_max <float> --data_min <string> --data_max <string> --misura ["Air Surface Temperature Anomaly", "Precipitation", "Outgoing Longwave Radiation"]
```

The `Query.py` file needs the input parameters, in particular the minimum and maximum latitude, the minimum and maximum longitude, the minimum and maximum date and the type of measurement you are looking for.

## Evaluation

To run the entire pipeline (extraction and query), we used two machines, the first with the following tecnichal specifications:

* **CPU**: 2 x CPU Intel(R) Xeon(R) Xeon 16-Core 5218 2.3Ghz 22MB. 
* **CORE NUMBER**: 32
* **RAM**: 192 GB

The second machine has the following tecnichal specification:

* **CPU**: 1 x CPU Intel(R) i3-4010U(R) 1.7 Ghz
* **CORE NUMBER**: 4
* **RAM**: 12 GB

## Contact
The [presentation](https://studentiuniparthenope-my.sharepoint.com/:p:/g/personal/gennaro_iannuzzo001_studenti_uniparthenope_it/EWgSGYEaG91GsIoOhfen-okBvRQszinhvJFNuc8GJeIKMw?e=nhRfJp) explains in detail the system.

For questions about code, please contact [pasqualedetrino](https://github.com/pasqualedetrino) or [GennaroIannuzzo](https://github.com/GennaroIannuzzo).

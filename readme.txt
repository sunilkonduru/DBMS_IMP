Implementing a Database:

This project is divided into 4 sub project and each subproject depends on its previous projects.

Project1: Paged File System

The PF component provides facilities for higher-level client components to perform file I/O in terms of pages. In the PF component, methods are provided to create, destroy, open, and close paged files, to read and write a specific page of a given file, and to add pages to a given file.

Project2: Record Manager(tuple-oriented file system):

Implemented a record manager on top of the basic paged file system in project 1.

Created a catalog to hold all information about the database. This includes the following:

1.Table info (name).
2.For each table, the columns, and for each of these columns: the column name, type and length
3.The name of the paged file in which the data corresponding to each table is stored.

Supported basic attribute types, including integers, reals, variable-length character strings. Tuples within file pages are represented using a record format that nicely handles mixes of binary data and variable-length data. "Nicely" here refers to both space and efficiency, e.g., you should not waste 70 bytes of space to store "abcdefghij" in a VARCHAR(80) field. In this project, Record representation allows direct addressibility of data fields - finding the nth field should be an O(1) operation, not an (n) operation.


Project3: Index Manager

Implemented an Indexing (IX) component. The IX component provides classes and methods for managing persistent indexes over unordered data records stored in files. The indexes ultimately will be used to speed up processing of relational selections, joins, and condition-based update and delete operations. Like the data records themselves, the indexes are also stored in files. Hence, in implementing the IX component, we used the file system that was implemented in project 1. In this database system architecture, you can think of the IX component and the record manager as sitting side by side above the file system.

The indexing technique you will implement in the IX component is B+ trees.


Project4: Query Evaluation

Implemented a Query Engine (QE) component. The QE component provides classes and methods for answering SQL queries.·Supported queries involving joins [nested loop, index and hash], select and project operators.

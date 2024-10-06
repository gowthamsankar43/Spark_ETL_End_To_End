Data Bricks End To End Project
End to End Spark project using Databricks with Unity Catalog feature
Source
 Datasets: https://www.kaggle.com/datasets/umairhayat/pizza-data-set-order-detail

Pipeline Flow 
 
Factory Pattern: 
Creational design pattern
The factory pattern in Python is a creational design pattern that allows the creation of objects without exposing the logic to the client. It is implemented using a factory method or a factory class to produce instances of objects based on input criteria or conditions
Workflow Application Notebook:
	Entry point of the Application which has a notebook trigger for ETL and user inputs. 

User Requirements: 
1.	Most sold pizza in Chicken Category month wise
2.	Most sold pizza in every Category overall
3.	Most sold pizza during 5.00pm to 10.00pm overall
4.	Most sold Large Pizza month wise
5.	Total amount of small Pizza sold in every category
6.	Total amount of Chicken Pizza sales in May month
7.	Total amount of Pizza sales in month wise every pizza sorted by max sold pizza
What is Object-Oriented Programming in Python?
In Python object-oriented Programming (OOPs) is a programming paradigm that uses objects and classes in programming. It aims to implement real-world entities like inheritance, polymorphisms, encapsulation, etc. in the programming. The main concept of object-oriented Programming (OOPs) or oops concepts in Python is to bind the data and the functions that work together as a single unit so that no other part of the code can access this data.
Python Class 
A class is a collection of objects. A class contains the blueprints or the prototype from which the objects are being created. It is a logical entity that contains some attributes and methods. 
Python Objects
In object-oriented programming Python, The object is an entity that has a state and behaviour associated with it. It may be any real-world object like a mouse, keyboard, chair, table, pen, etc. Integers, strings, floating-point numbers, even arrays, and dictionaries, are all objects.
An object consists of:
•	State: It is represented by the attributes of an object. It also reflects the properties of an object.
•	Behaviour: It is represented by the methods of an object. It also reflects the response of an object to other objects.
•	Identity: It gives a unique name to an object and enables one object to interact with other objects.
Python __init__ Method 
The __init__ method is similar to constructors in C++ and Java. It is run as soon as an object of a class is instantiated. The method is useful to do any initialization you want to do with your object.
 PySpark Broadcast Join
PySpark defines the pyspark.sql.functions.broadcast() to broadcast the smaller DataFrame which is then used to join the largest Data Frame. As you know PySpark splits the data into different nodes for parallel processing, when you have two DataFrames, the data from both are distributed across multiple nodes in the cluster so, when you perform traditional join, PySpark is required to shuffle the data. Shuffle is needed as the data for each joining key may not collocate on the same node and to perform join the data for each key should be brought together on the same node. Hence, the traditional PySpark Join is a very expensive operation.




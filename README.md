# sparkmicroservices

#Blog
<BR>
http://ashkrit.blogspot.com/2018/05/spark-microservices.html
<BR>
http://ashkrit.blogspot.sg/2018/05/custom-logs-in-apache-spark.html

#How to Build

mvn clean install

#How to start service

micro.main.QueryController

#How to test

http://localhost:8080/spark?text=select * from vehicle
<BR>
http://localhost:8080/spark?text=select * from vehicle where year = '2005'


#Multiple Query

http://localhost:8080/multiplequery - POST

Body 
<BR>
{
	"query" :[
		"select * From vehicle",
		"select * From vehicle where year='2005' "
		]
}
# sparkR-1
# test Databricks
# try-out with Databricks


> dim(faithful)   
> class(faithful)

[1] 272   2



# convert the built-in Faithful data set into SparkR DataFrame

> faithful_spark_df <- as.DataFrame(faithful)

#select one column
> head(select(faithful_spark_df, faithful_spark_df$eruptions))


#alternate way to select one column
> head(select(faithful_spark_df, "eruptions"))

# filtering in a dplyr syntax
> head(filter(faithful_spark_df, faithful_spark_df$eruptions > 3))



 eruptions waiting
1     3.600      79
2     3.333      74
3     4.533      85
4     4.700      88
5     3.600      85
6     4.350      85


# distinct
> mtcars_spark_df <- as.DataFrame(mtcars)
> head(mtcars_spark_df)


 mpg cyl disp  hp drat    wt  qsec vs am gear carb
1 21.0   6  160 110 3.90 2.620 16.46  0  1    4    4
2 21.0   6  160 110 3.90 2.875 17.02  0  1    4    4
3 22.8   4  108  93 3.85 2.320 18.61  1  1    4    1
4 21.4   6  258 110 3.08 3.215 19.44  1  0    3    1
5 18.7   8  360 175 3.15 3.440 17.02  0  0    3    2
6 18.1   6  225 105 2.76 3.460 20.22  1  0    3    1

> head(distinct(select(mtcars_spark_df,mtcars_spark_df$cyl)))


 cyl
1   8
2   4
3   6


# filter
> showDF(filter(mtcars_spark_df, mtcars_spark_df$hp>200))

+----+---+-----+-----+----+-----+-----+---+---+----+----+
| mpg|cyl| disp|   hp|drat|   wt| qsec| vs| am|gear|carb|
+----+---+-----+-----+----+-----+-----+---+---+----+----+
|14.3|8.0|360.0|245.0|3.21| 3.57|15.84|0.0|0.0| 3.0| 4.0|
|10.4|8.0|472.0|205.0|2.93| 5.25|17.98|0.0|0.0| 3.0| 4.0|
|10.4|8.0|460.0|215.0| 3.0|5.424|17.82|0.0|0.0| 3.0| 4.0|
|14.7|8.0|440.0|230.0|3.23|5.345|17.42|0.0|0.0| 3.0| 4.0|
|13.3|8.0|350.0|245.0|3.73| 3.84|15.41|0.0|0.0| 3.0| 4.0|
|15.8|8.0|351.0|264.0|4.22| 3.17| 14.5|0.0|1.0| 5.0| 4.0|
|15.0|8.0|301.0|335.0|3.54| 3.57| 14.6|0.0|1.0| 5.0| 8.0|
+----+---+-----+-----+----+-----+-----+---+---+----+----+


# arrange
> head(arrange(mtcars_spark_df,desc(mtcars_spark_df$mpg)))

 mpg cyl  disp  hp drat    wt  qsec vs am gear carb
1 33.9   4  71.1  65 4.22 1.835 19.90  1  1    4    1
2 32.4   4  78.7  66 4.08 2.200 19.47  1  1    4    1
3 30.4   4  75.7  52 4.93 1.615 18.52  1  1    4    2
4 30.4   4  95.1 113 3.77 1.513 16.90  1  1    5    2
5 27.3   4  79.0  66 4.08 1.935 18.90  1  1    4    1
6 26.0   4 120.3  91 4.43 2.140 16.70  0  1    5    2

# summarize
> head(summarize(groupBy(mtcars_spark_df, mtcars_spark_df$gear),count = n(mtcars_spark_df$gear)))

 gear count
1    4    12
2    3    15
3    5     5


# Piping
# install.packaes('magrittr')
> install.packages('magrittr')


Installing package into ‘/databricks/spark/R/lib’
(as ‘lib’ is unspecified)
trying URL 'http://cran.us.r-project.org/src/contrib/magrittr_1.5.tar.gz'
Content type 'application/x-gzip' length 200504 bytes (195 KB)
==================================================
downloaded 195 KB

* installing *source* package ‘magrittr’ ...
** package ‘magrittr’ successfully unpacked and MD5 sums checked
** R
** inst
** byte-compile and prepare package for lazy loading
** help
*** installing help indices
** building package indices
** installing vignettes
** testing if installed package can be loaded
* DONE (magrittr)
The downloaded source packages are in
	‘/tmp/RtmpehJmWx/downloaded_packages’
  
  
  #  Loan the library
  > library(magrittr)
  > groupBy(mtcars_spark_df, mtcars_spark_df$gear) %>% agg('mean_mpg'=mean(mtcars_spark_df$mpg)) %>% arrange(mtcars_spark_df$gear) %>% head
  
  gear mean_mpg
1    3 16.10667
2    4 24.53333
3    5 21.38000




# Linear Regression Model 
# Using iris data set
> iris_spark_df <- as.DataFrame(iris)
> head(iris_spark_df)

# Fit a linear model over the dataset
> model <- glm(Sepal_Length ~ Sepal_Width + Species, data = iris_spark_df, family = "gaussian")

# model coefficients are return in a similar format to R's native glm()
> summary(model)
> predictions <- predict(model, newData = iris_spark_df)
> head(select(predictions, "Sepal_Length", "prediction"))



  Sepal_Length prediction
1          5.1   5.063856
2          4.9   4.662076
3          4.7   4.822788
4          4.6   4.742432
5          5.0   5.144212
6          5.4   5.385281




# K Means Model

# Fit a k-means model with spark.kmeans
> irisDF <- suppressWarnings(createDataFrame(iris))
> kmeansDF <- irisDF
> kmeansTestDF <- irisDF
> kmeansModel <- spark.kmeans(kmeansDF, ~Sepal_Length + Sepal_Width+ Petal_Length + Petal_Width, k = 3)

# Model Summary
> summary(kmeansModel) 
# get fitted result from the k-means model
> showDF(fitted(kmeansModel))
# prediction
> kmeansPredictions<- predict(kmeansModel, kmeansTestDF)
> showDF(kmeansPredictions)



+------------+-----------+------------+-----------+-------+----------+
|Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|Species|prediction|
+------------+-----------+------------+-----------+-------+----------+
|         5.1|        3.5|         1.4|        0.2| setosa|         0|
|         4.9|        3.0|         1.4|        0.2| setosa|         0|
|         4.7|        3.2|         1.3|        0.2| setosa|         0|
|         4.6|        3.1|         1.5|        0.2| setosa|         0|
|         5.0|        3.6|         1.4|        0.2| setosa|         0|
|         5.4|        3.9|         1.7|        0.4| setosa|         0|
|         4.6|        3.4|         1.4|        0.3| setosa|         0|
|         5.0|        3.4|         1.5|        0.2| setosa|         0|
|         4.4|        2.9|         1.4|        0.2| setosa|         0|
|         4.9|        3.1|         1.5|        0.1| setosa|         0|
|         5.4|        3.7|         1.5|        0.2| setosa|         0|
|         4.8|        3.4|         1.6|        0.2| setosa|         0|
|         4.8|        3.0|         1.4|        0.1| setosa|         0|
|         4.3|        3.0|         1.1|        0.1| setosa|         0|
|         5.8|        4.0|         1.2|        0.2| setosa|         0|
|         5.7|        4.4|         1.5|        0.4| setosa|         0|
|         5.4|        3.9|         1.3|        0.4| setosa|         0|
|         5.1|        3.5|         1.4|        0.3| setosa|         0|
|         5.7|        3.8|         1.7|        0.3| setosa|         0|
|         5.1|        3.8|         1.5|        0.3| setosa|         0|
+------------+-----------+------------+-----------+-------+----------+
only showing top 20 rows
+------------+-----------+------------+-----------+-------+----------+
|Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|Species|prediction|
+------------+-----------+------------+-----------+-------+----------+
|         5.1|        3.5|         1.4|        0.2| setosa|         0|
|         4.9|        3.0|         1.4|        0.2| setosa|         0|
|         4.7|        3.2|         1.3|        0.2| setosa|         0|
|         4.6|        3.1|         1.5|        0.2| setosa|         0|
|         5.0|        3.6|         1.4|        0.2| setosa|         0|
|         5.4|        3.9|         1.7|        0.4| setosa|         0|
|         4.6|        3.4|         1.4|        0.3| setosa|         0|
|         5.0|        3.4|         1.5|        0.2| setosa|         0|
|         4.4|        2.9|         1.4|        0.2| setosa|         0|
|         4.9|        3.1|         1.5|        0.1| setosa|         0|
|         5.4|        3.7|         1.5|        0.2| setosa|         0|
|         4.8|        3.4|         1.6|        0.2| setosa|         0|
|         4.8|        3.0|         1.4|        0.1| setosa|         0|
|         4.3|        3.0|         1.1|        0.1| setosa|         0|
|         5.8|        4.0|         1.2|        0.2| setosa|         0|
|         5.7|        4.4|         1.5|        0.4| setosa|         0|
|         5.4|        3.9|         1.3|        0.4| setosa|         0|
|         5.1|        3.5|         1.4|        0.3| setosa|         0|
|         5.7|        3.8|         1.7|        0.3| setosa|         0|
|         5.1|        3.8|         1.5|        0.3| setosa|         0|
+------------+-----------+------------+-----------+-------+----------+
only showing top 20 rows
